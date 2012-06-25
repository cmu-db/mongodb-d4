# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
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

import random
import logging
from pprint import pformat

import catalog
from util import constants
from workload import OpHasher

LOG = logging.getLogger(__name__)

## ==============================================
## Abstract Convertor
## ==============================================
class AbstractConverter():
    
    def __init__(self, metadata_db, dataset_db):
        self.metadata_db = metadata_db
        self.dataset_db = dataset_db

        # The WORKLOAD collection is where we stores sessions+operations
        self.workload_col = self.metadata_db[constants.COLLECTION_WORKLOAD]

        # The SCHEMA collection is where we will store the metadata information that
        # we will derive from the RECREATED database
        self.schema_col = self.metadata_db[constants.COLLECTION_SCHEMA]

        self.stop_on_error = False
        self.limit = None
        self.skip = None
        self.clean = None
        
        self.no_load = False
        self.no_reconstruct = False
        self.no_sessionizer = False

        self.total_queries = 0
        self.hasher = OpHasher()
    ## DEF
        
    def processImpl(self):
        raise NotImplementedError("Unimplemented %s.process()" % (self.__init__.im_class))
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

    def process(self, page_size=4):
        self.processImpl()

        # The PostProcessor performs final calculations on an already loaded metadata
        # and workload database. These calculations can be computed regardless of whether
        # the catalog information came from MongoSniff or MySQL.
        # This should only be invoked once after do the initial loading.

        # STEP 1: Add query hashes
        self.addQueryHashes()

        # STEP 2: Compute statistics about collections
        self.computeCollectionStats()

        # STEP 3: Process workload
        self.computeWorkloadStats()

        # STEP 4: Process dataset
        self.computeDatasetStats()

        # Finalize workload percentage statistics for each collection
        page_size *= 1024
        for col in self.metadata_db.Collection.find():
            col['workload_percent'] = col['workload_queries'] / float(self.total_queries)
            col['max_pages'] = col['tuple_count'] * col['avg_doc_size'] / page_size
        ## FOR

    ## DEF

    def addQueryHashes(self):
        sessions = self.metadata_db[constants.COLLECTION_WORKLOAD].find()
        if self.limit: sessions.limit(self.limit)

        for sess in sessions:
            for op in sess['operations'] :
                op["query_hash"] = self.hasher.hash(op)
            self.metadata_db[constants.COLLECTION_WORKLOAD].save(sess)
            ## FOR
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Query Class Histogram:\n%s" % self.hasher.histogram)
    ## DEF

    def computeCollectionStats(self):
        """Gather statistics from an iterable of collections for using in
           instantiation of the cost model and for determining the initial
           design solution"""

        for col in self.metadata_db[constants.COLLECTION_SCHEMA].find():
            for field, data in col['fields'].iteritems() :
                if data['query_use_count'] > 0 and not field in col['interesting']:
                    col['interesting'].append(field)
                    ## FOR
            ## FOR
        return
        ## FOR

    def computeWorkloadStats(self):
        """Process Workload Trace"""

        for sess in self.metadata_db.Session.fetch():
            start_time = None
            end_time = None

            for op in sess['operations']:
                # We need to know the total number of queries that we've seen
                self.total_queries += 1

                # The start_time is the timestamp of when the first query occurs
                if not start_time: start_time = op['query_time']
                start_time = min(start_time, op['query_time'])

                # The end_time is the timestamp of when the last response arrives
                if 'resp_time' in op and op['resp_time']: end_time = max(end_time, op['resp_time'])

                # Get the collection information object
                # We will use this to store the number times each key is referenced in a query
                col_info = self.metadata_db.Collection.one({'name': op['collection']})
                if not col_info:
                    col_info = self.metadata_db.Collection()
                    col_info['name'] = op['collection']
                col_info['workload_queries'] += 1

                if not 'predicates' in op or not op['predicates']:
                    op['predicates'] = { }

                try:
                    # QUERY
                    if op['type'] == constants.OP_TYPE_QUERY:
                        for content in op['query_content'] :
                            if '#query' in content and content['#query'] is not None:
                                self.processOpFields(col_info, op, content['#query'])

                    # DELETE
                    elif op['type'] == constants.OP_TYPE_DELETE:
                        for content in op['query_content']:
                            self.processOpFields(col_info, op, content)

                    # INSERT
                    elif op['type'] == constants.OP_TYPE_INSERT:
                        for content in op['query_content']:
                            for k,v in content.iteritems():
                                self.processOpFields(col_info, op, content)

                    # UPDATE
                    elif op['type'] == constants.OP_TYPE_UPDATE:
                        for content in op['query_content']:
                            self.processOpFields(col_info, op, content)
                except:
                    LOG.error("Unexpected error for operation #%d in Session #%d\n%s", \
                              op['query_id'], sess['session_id'], pformat(op))
                    raise

                try:
                    col_info.save() # self.metadata_db.Collection.save()
                except:
                    col_info['fields'] = None
                    LOG.error("\n" + pformat(col_info))
                    raise
            ## FOR (operations)

            if start_time and end_time:
                sess['start_time'] = start_time
                sess['end_time'] = end_time

            LOG.debug("Updating Session #%d" % sess['session_id'])
            sess.save()
        ## FOR (sessions)
    ## DEF

    def processOpFields(self, col_info, op, content):
        for k,v in content.iteritems():
            # Skip anything that starts with our special char
            # Those are flag markers used by MongoDB's queries
            if k.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX):
                continue

            # We need to add the field to the collection if it doesn't
            # already exist. This will occur if this op was an aggregate,
            # which we ignore when recreating the schema
            if not k in col_info['fields']:
                fieldType = catalog.fieldTypeToString(type(v))
                col_info['fields'][k] = catalog.Collection.fieldFactory(k, fieldType)

            f = col_info['fields'][k]
            f['query_use_count'] += 1

            # No predicate for insert operations
            # No projections for insert operations
            if op['type'] != constants.OP_TYPE_INSERT:
                # Update how this key was used with predicates
                # TODO: This doesn't seem right because it will overwrite whatever we had there last?
                if type(v) == dict:
                    op['predicates'][k] = constants.PRED_TYPE_RANGE
                else:
                    op['predicates'][k] = constants.PRED_TYPE_EQUALITY
        ## FOR

        return
    ## DEF

    def computeDatasetStats(self, sample_rate = 100):
        """Process Sample Dataset"""
        tuple_sizes = {}
        distinct_values = {}
        first = {}

        # Compute per-collection statistics
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
            for row in self.dataset_db[col['name']].find():
                col['tuple_count'] += 1
                if random.randint(0, 100) <= sample_rate :
                    for k, v in row.iteritems() :
                        if k <> '_id' :
                            size = catalog.getEstimatedSize(col['fields'][k]['type'], v)
                            tuple_sizes[col['name']] += size
                            distinct_values[col['name']][k][v] = v
                        else :
                            tuple_sizes[col['name']] += 12
            if not col['tuple_count']:
                col['avg_doc_size'] = 0
            else :
                col['avg_doc_size'] = int(tuple_sizes[col['name']] / col['tuple_count'])

            # Calculate cardinality and selectivity
            for k,v in col['fields'].iteritems() :
                v['cardinality'] = len(distinct_values[col['name']][k])
                if not col['tuple_count']:
                    v['selectivity'] = 0
                else :
                    v['selectivity'] = v['cardinality'] / col['tuple_count']
                ## FOR

            col.save()
            ## FOR
    ## DEF

## CLASS


    
