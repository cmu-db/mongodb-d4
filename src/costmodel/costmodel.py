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
from __future__ import division

import sys
import json
import logging
import math
import random

from util import constants

LOG = logging.getLogger(__name__)

'''
Cost Model object

Used to evaluate the "goodness" of a design in respect to a particular workload. The 
Cost Model uses Network Cost, Disk Cost, and Skew Cost functions (in addition to some
configurable coefficients) to determine the overall cost for a given design/workload
combination

collections : CollectionName -> Collection
workload : List of Sessions

config {
    'weight_network' : Network cost coefficient,
    'weight_disk' : Disk cost coefficient,
    'weight_skew' : Skew cost coefficient,
    'nodes' : Number of nodes in the Mongo DB instance,
    'max_memory' : Amount of memory per node in MB,
    'address_size' : Amount of memory required to index 1 document,
    'skew_intervals' : Number of intervals over which to calculate the skew costs
}
'''
class CostModel(object):
    
    def __init__(self, collections, workload, config):
        assert type(collections) == dict
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.collections = collections
        self.workload = workload

        self.rg = random.Random()
        self.rg.seed('cost model coolness')

        self.weight_network = config.get('weight_network', 1.0)
        self.weight_disk = config.get('weight_disk', 1.0)
        self.weight_skew = config.get('weight_skew', 1.0)
        self.nodes = config.get('nodes', 1)

        # Convert MB to KB
        self.max_memory = config['max_memory'] * 1024 * 1024 * self.nodes
        self.skew_segments = config['skew_intervals'] # Why? "- 1"
        self.address_size = config['address_size'] / 4

        self.splitWorkload()
    ## DEF

    def splitWorkload(self):
        """Divide the workload up into segments for skew analysis"""
        if len(self.workload) > 0 :
            start_time = self.workload[0]['start_time']
            end_time = None
            i = len(self.workload)-1
            while i >= 0 and not end_time:
                end_time = self.workload[i]['end_time']
                i -= 1
            assert start_time
            assert end_time
        else:
            return 0

        LOG.info("Workload Segments - START:%d / END:%d", start_time, end_time)
        self.workload_segments = [ [] for i in xrange(0, self.skew_segments) ]
        for sess in self.workload:
            idx = self.getSessionSegment(sess, start_time, end_time)
            self.workload_segments[idx].append(sess)
        ## FOR
    ## DEF

    def getSessionSegment(self, sess, start_time, end_time):
        """Return the segment offset that the given Session should be assigned to"""
        timestamp = sess['start_time']
        if timestamp == end_time: timestamp -= 1
        ratio = (timestamp - start_time) / float(end_time - start_time)
        return int(self.skew_segments * ratio)
    ## DEF

    
    def overallCost(self, design):
        cost = 0
        cost += self.weight_network * self.networkCost(design)
        cost += self.weight_disk * self.diskCost(design)
        cost += self.weight_skew * self.skewCost(design)
        return cost / float(self.weight_network + self.weight_disk + self.weight_skew)
    ## DEF
    
    def networkCost(self, design) :
        cost, queries = self.partialNetworkCost(design, self.workload)
        return cost
    ## DEF
    
    def diskCost(self, design):
        """
        Estimate the Disk Cost for a design and a workload
        - Best case, every query is satisfied by main memory
        - Worst case, every query requires a full collection
        """
        worst_case = 0
        cost = 0
        # 1. estimate index memory requirements
        index_memory = self.getIndexSize(design)
        if index_memory > self.max_memory :
            return 10000000000000
        
        # 2. approximate the number of documents per collection in the working set
        working_set = self.estimateWorkingSets(design, self.max_memory - index_memory)
        
        # 3. Iterate over workload, foreach query:
        for sess in self.workload:
            for op in sess['operations'] :
                # is the collection in the design - if not ignore
                if design.hasCollection(op['collection']) == False :
                    break
                
                # Does this depend on the type of query? (insert vs update vs delete vs select)
                multiplier = 1
                if op['type'] == constants.OP_TYPE_INSERT:
                    multiplier = 2
                    max_pages = 1
                    min_pages = 1
                    pass
                else:
                    if op['type'] in [constants.OP_TYPE_UPDATE, constants.OP_TYPE_DELETE]:
                        multiplier = 2
                    ## end if ##
                    
                    # How many pages for the queries tuples?
                    max_pages = self.collections[op['collection']]['max_pages']
                    min_pages = max_pages
                    
                    # Is the entire collection in the working set?
                    if working_set[op['collection']] >= 100 :
                        min_pages = 0
                    
                    # Does this query hit an index?
                    elif design.hasIndex(op['collection'], list(op['predicates'])) :
                        min_pages = 0
                    # Does this query hit the working set?
                    else:
                        # TODO: Complete hack! This just random guesses whether its in the
                        #       working set! This is not what we want to do!
                        ws_hit = self.rg.randint(1, 100)
                        if ws_hit <= working_set[op['collection']] :
                            min_pages = 0
                ## end if ##
                    
                cost += min_pages        
                worst_case += max_pages
        if not worst_case:
            return 0
        else:
            return cost / worst_case
    ## DEF
    
    def skewCost(self, design):
        """Calculate the network cost for each segment for skew analysis"""
        segment_costs = []
        
        for i in range(0, len(self.workload_segments)) :
            segment_costs.append(self.partialNetworkCost(design, self.workload_segments[i]))
        
        # Determine overall skew cost as a function of the distribution of the
        # segment network costs
        sum_of_query_counts = 0
        sum_intervals = 0
        for i in range(0, len(self.workload_segments)) :
            skew = 1 - segment_costs[i][0]
            sum_intervals += skew * segment_costs[i][1]
            sum_of_query_counts += segment_costs[i][1]
        
        if not sum_of_query_counts:
            return 0
        else:
            return sum_intervals / sum_of_query_counts
    ## DEF
        
    def partialNetworkCost(self, design, segment):
        if self.debug: LOG.debug("Computing network cost for %d sessions", len(segment))
        result = 0
        query_count = 0
        for sess in segment:
            previous_op = None
            for op in sess['operations']:
                # Check to see if the queried collection exists in the design's 
                # de-normalization scheme

                # Collection is not in design.. don't count query
                if not design.hasCollection(op['collection']):
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s", \
                                             op['type'], op['query_id'], op['collection'])
                    continue

                # Check whether this collection is embedded inside of another
                # TODO: Need to get ancestor
                parent_col = design.getDenormalizationParent(op['collection'])
                if self.debug and parent_col:
                    LOG.debug("Op #%d on '%s' Parent Collection -> '%s'", \
                              op["query_id"], op["collection"], parent_col)

                process = False
                # This is the first op we've seen in this session
                if not previous_op:
                    process = True
                # Or this operation's target collection is not embedded
                elif not parent_col:
                    process = True
                # Or if either the previous op or this op was not a query
                elif previous_op['type'] <> constants.OP_TYPE_QUERY or op['type'] <> constants.OP_TYPE_QUERY:
                    process = True
                # Or if the previous op was
                elif previous_op['collection'] <> parent_col:
                    process = True
                # TODO: What if the previous op should be merged with a later op?
                #       We would lose it because we're going to overwrite previous op

                # Process this op!
                if process:
                    query_count += 1

                    # Inserts always go to a single node
                    if op['type'] == constants.OP_TYPE_INSERT:
                        result += 1
                    # Network costs of SELECT, UPDATE, DELETE queries are based off
                    # of using the sharding key in the predicate
                    else:
                        if len(op['predicates']) > 0:
                            scan = True
                            predicate_type = None
                            for k,v in op['predicates'].iteritems() :
                                if design.inShardKeyPattern(op['collection'], k) :
                                    scan = False
                                    predicate_type = v
                            if self.debug:
                                LOG.debug("Op #%d Predicates: %s [scan=%s / predicateType=%s]", \
                                          op['query_id'], op['predicates'], scan, predicate_type)
                            if not scan:
                                # Query uses shard key... need to determine if this is an
                                # equality predicate or a range type
                                if predicate_type == constants.PRED_TYPE_EQUALITY:
                                    result += 0.0
                                else:
                                    nodes = self.guessNodes(design, op['collection'], k)
                                    LOG.info("Estimating that Op #%d on '%s' touches %d nodes", \
                                             op["query_id"], op["collection"], nodes)
                                    result += nodes
                            else:
                                if self.debug:
                                    LOG.debug("Op #%d on '%s' is a broadcast query", \
                                              op["query_id"], op["collection"])
                                result += self.nodes
                        else:
                            result += self.nodes
                else:
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s [parent=%s / previous=%s]", \
                                             op['type'], op['query_id'], op['collection'], \
                                             parent_col, (previous_op != None))
                ## IF
                previous_op = op
        if not query_count:
            cost = 0
        else:
            cost = result / float(query_count * self.nodes)

        LOG.info("Computed Network Cost: %f [result=%d / queryCount=%d]", \
                 cost, result, query_count)

        return (cost, query_count)
        
    def guessNodes(self, design, colName, key) :
        """
        Serve as a stand-in for the EXPLAIN function referenced in the paper?

        How do we use the statistics to determine the selectivity of this particular
        attribute and thus determine the number of nodes required to answer the query?
        """
        return math.ceil(self.collections[colName]['fields'][key]['selectivity'] * self.nodes)
        
    '''
    Estimate the amount of memory required by the indexes of a given design
    '''
    def getIndexSize(self, design) :
        memory = 0
        for colName in design.getCollections() :
            # Add a hit for the index on '_id' attribute for each collection
            memory += self.collections[colName]['doc_count'] * self.collections[colName]['avg_doc_size']
            
            # Process other indexes for this collection in the design
            for index in design.getIndexes(colName) :
                memory += self.collections[colName]['doc_count'] * self.address_size * len(index)
        return memory
        
    '''
    Estimate the percentage of a collection that will fit in working set space
    '''
    def estimateWorkingSets(self, design, capacity) :
        working_set_counts = {}
        leftovers = {}
        buffer = 0
        needs_memory = []
        
        # create tuples of workload percentage, collection for sorting
        sorting_pairs = []
        for col in design.getCollections() :
            sorting_pairs.append((self.collections[col]['workload_percent'], col))
        sorting_pairs.sort(reverse=True)
        
        # iterate over sorted tuples to process in descending order of usage
        for pair in sorting_pairs :
            memory_available = capacity * pair[0]
            memory_needed = self.collections[pair[1]]['avg_doc_size'] * self.collections[pair[1]]['doc_count']
            
            # is there leftover memory that can be put in a buffer for other collections?
            if memory_needed <= memory_available :
                working_set_counts[pair[1]] = 100
                buffer += memory_available - memory_needed
            else:
                col_percent = memory_available / memory_needed
                still_needs = 1.0 - col_percent
                working_set_counts[pair[1]] = math.ceil(col_percent * 100)
                needs_memory.append((still_needs, pair[1]))
        
        # This is where the problem is... Need to rethink how I am doing this.
        for pair in needs_memory :
            memory_available = buffer
            memory_needed = (1 - (working_set_counts[pair[1]] / 100)) * \
                            self.collections[pair[1]]['avg_doc_size'] * \
                            self.collections[pair[1]]['doc_count']
            
            if memory_needed <= memory_available :
                working_set_counts[pair[1]] = 100
                buffer = memory_available - memory_needed
            else:   
                if memory_available > 0 :
                    col_percent = memory_available / memory_needed
                    working_set_counts[pair[1]] += col_percent * 100
        return working_set_counts
    ## DEF
## end class ##