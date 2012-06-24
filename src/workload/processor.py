# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import sys
import random
import logging
from pprint import pformat

import catalog
from ophasher import *
from stats import Stats
from util import constants

LOG = logging.getLogger(__name__)

## ==============================================
## Workload Processor
## ==============================================
class Processor:

    def __init__(self, metadata_db, dataset_db):
        self.metadata_db = metadata_db
        self.dataset_db = dataset_db
        self.hasher = OpHasher()
        
        self.op_limit = None
    ## DEF
    
    def reset(self):
        # FIXME: This should be done with a single update query
        for col in self.metadata_db.Collection.find():
            for k, v in col['fields'].iteritems() :
                v['query_use_count'] = 0
                v['cardinality'] = 0
                v['selectivity'] = 0
            col.save()
    ## DEF
    
    def process(self):
        # STEP 1: Add query hashes
        self.addQueryHashes()
        
        # STEP 2: Compute statistics about collections
        self.computeCollectionStats()
        
        # STEP 3: Process workload
        self.processWorkload()
        
        # STEP 4: Process dataset
        self.processDataset()
        
    ## DEF
    
    def addQueryHashes(self):
        sessions = self.metadata_db[constants.COLLECTION_WORKLOAD].find()
        if self.op_limit: sessions.limit(self.op_limit)
        
        for sess in sessions:
            for op in sess['operations'] :
                op[u"query_hash"] = self.hasher.hash(op)
            self.metadata_db[constants.COLLECTION_WORKLOAD].save(sess)
        ## FOR
        LOG.debug("Query Class Histogram:\n%s" % self.hasher.histogram)
    ## DEF
    
    def computeCollectionStats(self):
        '''Gather statistics from an iterable of collections for using in 
           instantiation of the cost model and for determining the initial
           design solution'''
           
        collections = self.metadata_db[constants.COLLECTION_SCHEMA].find()
        for col in collections:
            stats = Stats()
            stats.name = col['name']
            stats.tuple_count = col['tuple_count']
            stats.avg_doc_size = col['avg_doc_size']
            
            for field, data in col['fields'].iteritems() :
                if data['query_use_count'] > 0:
                    stats.interesting.append(field)
                field = {
                    'query_use_count': data['query_use_count'],
                    'cardinality': data['cardinality'],
                    'selectivity': data['selectivity'],
                }
                stats.fields.append(field)
            ## FOR
            
            stats.save()
        ## FOR
        return
    ## FOR
    
    def processWorkload(self):
        """Process Workload Trace"""
        
        sessions = self.metadata_db[constants.COLLECTION_WORKLOAD].find()
        if self.op_limit: sessions.limit(self.op_limit)
        
        for sess in sessions:
            start_time = None
            end_time = None
            
            for op in sess['operations']:
                # The start_time is the timestamp of when the first query occurs
                if start_time == None: start_time = op.query_time
                start_time = min(start_time, op.query_time)
    
                # The end_time is the timestamp of when the last response arrives                
                if op.resp_time: end_time = max(end_time, op.resp_time)
                
                # Get the collection information object
                # We will use this to store the number times each key is referenced in a query
                col_info = self.metadata_db.Collection.one({'name': op['collection']})
                
                if op.predicates == None: op.predicates = { }
                
                # FIXME - We ware suppose to update these counters somewhere
                # FIXME - Where is this suppose to be stored?
                # statistics[op['collection']]['workload_queries'] += 1
                # statistics['total_queries'] += 1
                
                # DELETE
                if op['type'] == constants.OP_TYPE_DELETE:
                    for content in op['query_content'] :
                        for k,v in content.iteritems() :
                            tuples.append((k, v))
                            col_info['fields'][k]['query_use_count'] += 1
                            if type(v) == dict:
                                op.predicates[k] = constants.PRED_TYPE_RANGE
                            else:
                                op.predicates[k] = constants.PRED_TYPE_EQUALITY
                    ## FOR
                # INSERT
                elif op['type'] == constants.OP_TYPE_INSERT:
                    for content in op['query_content'] :
                        for k,v in content.iteritems() :
                            tuples.append((k, v))
                            col_info['fields'][k]['query_use_count'] += 1

                    # No predicate for insert operations
                    # No projections for insert operations
                            
                # QUERY
                elif op['type'] == constants.OP_TYPE_QUERY:
                    for content in op['query_content'] :
                        if content['query'] != None :
                            for k, v in content['query'].iteritems() :
                                tuples.append((k, v))
                                col_info['fields'][k]['query_use_count'] += 1
                                if type(v) == dict:
                                    op.predicates[k] = constants.PRED_TYPE_RANGE
                                else:
                                    op.predicates[k] = constants.PRED_TYPE_EQUALITY
                    ## FOR
                    
                # UPDATE
                elif op['type'] == constants.OP_TYPE_UPDATE:
                    for content in op['query_content'] :
                        try :
                            for k,v in content.iteritems() :
                                tuples.append((k, v))
                                col_info['fields'][k]['query_use_count'] += 1
                                if type(v) == dict:
                                    op.predicates[k] = constants.PRED_TYPE_RANGE
                                else:
                                    op.predicates[k] = constants.PRED_TYPE_EQUALITY
                        except AttributeError :
                            # Why?
                            pass
                    ## FOR
                
                col_info.save()
                
            ## FOR (operations)
            
            if start_time != None and end_time != None:
                sess.start_time = start_time
                sess.end_time = end_time
            
            LOG.debug("Updating Session #%d" % sess.session_id)
            sess.save()
            
        ## FOR (sessions)
    ## DEF
    
    def processDataset(self, sample_rate = 100):
        """Process Sample Dataset"""
        tuple_sizes = {}
        distinct_values = {}
        first = {}
        
        # Compute per-column statistics
        for col in self.metadata_db.Collection.find():
            if not col['name'] in distinct_values:
                distinct_values[col['name']] = {}
                first[col['name']] = {}
                for k, v in col['fields'].iteritems() :
                    distinct_values[col['name']][k] = {}
                    first[col['name']][k] = True
                ## FOR
            ## IF
            
            col['tuple_count'] = 0
            tuple_sizes[col['name']] = 0
            cursor = self.dataset_db[col['name']].find()
            for row in cursor:
                col['tuple_count'] += 1
                to_use = random.randrange(1, 100, 1)
                if to_use <= sample_rate : 
                    for k, v in row.iteritems() :
                        if k <> '_id' :
                            size = catalog.getEstimatedSize(col['fields'][k]['type'], v)
                            tuple_sizes[col['name']] += size
                            distinct_values[col['name']][k][v] = v
                        else :
                            tuple_sizes[col['name']] += 12
            if col['tuple_count'] == 0 :
                col['avg_doc_size'] = 0
            else :
                col['avg_doc_size'] = int(tuple_sizes[col['name']] / col['tuple_count'])
                
            # Calculate cardinality and selectivity
            for k,v in col['fields'].iteritems() :
                v['cardinality'] = len(distinct_values[col['name']][k])
                if col['tuple_count'] == 0 :
                    v['selectivity'] = 0
                else :
                    v['selectivity'] = v['cardinality'] / col['tuple_count']
            ## FOR
            
            col.save()
        ## FOR
    ## DEF
    
## CLASS