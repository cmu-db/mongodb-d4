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

import logging
import math
import random
from pprint import pformat
import catalog

import workload
from lrubuffer import LRUBuffer
from nodeestimator import NodeEstimator
from util import constants
from util import Histogram

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
        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.collections = collections
        self.workload = workload

        self.weight_network = config.get('weight_network', 1.0)
        self.weight_disk = config.get('weight_disk', 1.0)
        self.weight_skew = config.get('weight_skew', 1.0)
        self.num_nodes = config.get('nodes', 1)

        # Convert MB to KB
        self.max_memory = config['max_memory'] * 1024 * 1024
        self.skew_segments = config['skew_intervals'] # Why? "- 1"
        self.address_size = config['address_size'] / 4

        self.estimator = NodeEstimator(self.collections, self.num_nodes)

        self.buffers = [ ]
        for i in xrange(self.num_nodes):
            lru = LRUBuffer(self.collections, self.max_memory)
            self.buffers.append(lru)

        self.splitWorkload()
    ## DEF

    def overallCost(self, design):
        cost = 0
        cost += self.weight_network * self.networkCost(design)
        cost += self.weight_disk * self.diskCost(design)
        cost += self.weight_skew * self.skewCost(design)
        return cost / float(self.weight_network + self.weight_disk + self.weight_skew)
    ## DEF

    ## -----------------------------------------------------------------------
    ## DISK COST
    ## -----------------------------------------------------------------------

    def diskCost(self, design):
        """
            Estimate the Disk Cost for a design and a workload
        """
        # Worst case is when every query requires a full collection scan
        # Best case, every query is satisfied by main memory
        cost = 0
        worst_case = 0

        # (1) Initialize all of the LRU buffers
        for lru in self.buffers:
            lru.reset()
            lru.initialize(design)

        # (2) Ok strap on your helmet, this is the magical part of the whole thing!
        #
        # Outline:
        # + For each operation, we need to figure out what document(s) it's going
        #   to need to touch. From this we want to compute a unique hash signature
        #   for those document so that we can identify what node those documents
        #   reside on and whether those documents are in our working set memory.
        #
        # + For each node, we are going to have a single LRU buffer that simulates
        #   the working set for all collections and indexes in the database.
        #   Documents entries are going to be tagged based on whether they are
        #   part of an index or a collection.
        #
        # + Now when we iterate through each operation in our workload, we are
        #   going to need to first figure out what index (if any) it will need
        #   to use and how it will be used (i.e., equality look-up or range scan).
        #   We can then compute the hash for the look-up keys.
        #   If that key is in the LRU buffer, then we will update its entry's last
        #   accessed timestamp. If it's not, then we will increase the page hit
        #   counter and evict some other entry.
        #   After evaluating the target index, we will check whether the index
        #   covers the query. If it does, then we're done
        #   If not, then we need to compute hash for the "base" documents that it
        #   wants to access (i.e., in the collection). Then just as before, we
        #   will check whether its in our buffer, make an eviction if not, and
        #   update our page hit counter.
        #   There are several additional corner cases that we need to handle:
        #      INSERT/UPDATE: Check whether it's an upsert query
        #      INSERT/UPDATE/DELETE: We assume that they're using a WAL and therefore
        #                            writing dirty pages is "free"
        #      UPDATE/DELETE: Check whether the "multi" flag is set to true, which will
        #                     tell us to stop the scan after the first matching document
        #                     is found.
        #
        # NOTE: We don't need to keep track of evicted tuples. It's either in the LRU buffer or not.
        # TODO: We may want to figure out how to estimate whether we are traversing
        #       indexes on the right-hand side of the tree. We could some preserve
        #       the sort order the keys when we hash them...

        cost = 0
        for sess in self.workload:
            for op in sess['operations']:
                # is the collection in the design - if not ignore
                if not design.hasCollection(op['collection']):
                    continue
                col_info = self.collections[op['collection']]

                indexKeys, covering = self.guessIndex(design, op)
                pageHits = 0

                # Grab all of the query contents
                for content in workload.getOpContents(op):
                    for node_id in self.estimator.estimateOp(design, op):
                        lru = self.buffers[node_id]

                        # If we have a target index, hit that up
                        if indexKeys:
                            # TODO: Need to handle whether it's a scan or an equality predicate
                            documentIds = [ hash(catalog.getFieldValues(indexKeys, content)) ]
                            pageHits += lru.getDocumentsFromIndex(op['collection'], indexKeys, documentIds)

                        # If it's not a covering index, then we need to hit up
                        # the collection to retrieve the whole document
                        if not covering:
                            # TODO: Need to handle whether it's a scan or an equality predicate
                            documentIds = [ hash(catalog.getAllValues(content)) ]
                            pageHits += lru.getDocumentsFromCollection(op['collection'], documentIds)
                    ## FOR (node)
                ## FOR (content)
                cost += pageHits

                if op['type'] == constants.OP_TYPE_INSERT:
                    # The worst case for an insert is if we have to a evict
                    # another record to make space for our new one
                    worst_case += 1
                else:
                    # The worst case for all other operations is if we have to do
                    # a full table scan that requires us to evict the entire buffer
                    # Hence, we multiple the max pages by two
                    worst_case += col_info['max_pages'] * 2

                if self.debug:
                    LOG.debug("Op #%d on '%s' -> PAGES[hits:%d / worst:%d]", \
                              op["query_id"], op["collection"], pageHits, worst_case)
            ## FOR (op)
        ## FOR (sess)
        if not worst_case:
            return 0
        else:
            return cost / worst_case
    ## DEF

    def guessIndex(self, design, op):

        # TODO: We should cache this selection based on query_hashes

        # Simply choose the index that has most of the fields
        # referenced in the operation.
        indexes = design.getIndexes(op['collection'])
        op_contents = workload.getOpContents(op)
        best_index = None
        best_ratio = None
        for i in xrange(len(indexes)):
            field_cnt = 0
            for indexKey in indexes[i]:
                if catalog.hasField(indexKey, op_contents):
                    field_cnt += 1
            field_ratio = field_cnt / float(len(indexes[i]))
            if not best_index or field_ratio >= best_ratio:
                # If the ratios are the same, then choose the
                # one with the most keys
                if field_ratio == best_ratio:
                    if len(indexes[i]) < len(best_index):
                        continue
                best_index = indexes[i]
                best_ratio = field_ratio
        ## FOR
        LOG.info("Op #%d - BestIndex:%s / BestRatio:%s", \
                 op['query_id'], best_index, best_ratio)

        # Check whether this is a covering index
        covering = False
        if best_index and op['type'] == constants.OP_TYPE_QUERY:
            # The second element in the query_content is the projection
            if len(op['query_content']) > 1:
                projectionFields = op['query_content'][1]
            # Otherwise just check whether the index encompasses all fields
            else:
                col_info = self.collections[op['collection']]
                projectionFields = col_info['fields']

            if projectionFields and len(best_index) == len(projectionFields):
                # FIXME
                covering = True
        ## IF
        return best_index, covering
    ## DEF

    def estimateWorkingSets(self, design, capacity):
        '''
            Estimate the percentage of a collection that will fit in working set space
        '''
        working_set_counts = {}
        leftovers = {}
        buffer = 0
        needs_memory = []

        if self.debug:
            LOG.debug("Estimating collection working sets [capacity=%d]", capacity)

        # iterate over sorted tuples to process in descending order of usage
        for col_name in sorted(self.collections.keys(), key=lambda k: self.collections[k]['workload_percent'], reverse=True):
            col_info = self.collections[col_name]
            memory_available = capacity * col_info['workload_percent']
            memory_needed = col_info['avg_doc_size'] * col_info['doc_count']

            if self.debug:
                LOG.debug("%s Memory Needed: %d", col_name, memory_needed)

            # is there leftover memory that can be put in a buffer for other collections?
            if memory_needed <= memory_available :
                working_set_counts[col_name] = 100
                buffer += memory_available - memory_needed
            else:
                col_percent = memory_available / memory_needed
                still_needs = 1.0 - col_percent
                working_set_counts[col_name] = math.ceil(col_percent * 100)
                needs_memory.append((still_needs, col_name))

        # This is where the problem is... Need to rethink how I am doing this.
        for still_needs, col_info in needs_memory:
            memory_available = buffer
            memory_needed = (1 - (working_set_counts[col_name] / 100)) *\
                            self.collections[col_name]['avg_doc_size'] *\
                            self.collections[col_name]['doc_count']

            if memory_needed <= memory_available :
                working_set_counts[col_name] = 100
                buffer = memory_available - memory_needed
            elif memory_available > 0 :
                col_percent = memory_available / memory_needed
                working_set_counts[col_name] += col_percent * 100
        return working_set_counts
    ## DEF

    ## -----------------------------------------------------------------------
    ## SKEW COST
    ## -----------------------------------------------------------------------

    def skewCost(self, design):
        """Calculate the network cost for each segment for skew analysis"""

        op_counts = [ 0 ] *  self.skew_segments
        segment_skew = [ 0 ] *  self.skew_segments
        for i in range(0, len(self.workload_segments)):
            # TODO: We should cache this so that we don't have to call it twice
            segment_skew[i], op_counts[i] = self.calculateSkew(design, self.workload_segments[i])

        weighted_skew = sum([segment_skew[i] * op_counts[i] for i in xrange(len(self.workload_segments))])
        return weighted_skew / float(sum(op_counts))
    ## DEF

    def calculateSkew(self, design, segment):
        """
            Calculate the cluster skew factor for the given workload segment
            See Alg.#3 from Pavlo et al. 2012:
            http://hstore.cs.brown.edu/papers/hstore-partitioning.pdf
        """
        if self.debug:
            LOG.debug("Computing skew cost for %d sessions over %d segments", len(segment), self.skew_segments)

        # Keep track of how many times that we accessed each node
        nodeCounts = Histogram()

        # Iterate over each session and get the list of nodes
        # that we estimate that each of its operations will need to touch
        query_count = 0
        for sess in segment:
            for op in sess['operations']:
                # Skip anything that doesn't have a design configuration
                if not design.hasCollection(op['collection']):
                    if self.debug:
                        LOG.debug("SKIP - %s Op #%d on %s",\
                                  op['type'], op['query_id'], op['collection'])
#                    if self.debug: LOG.debug("SKIP: ")
                    continue

                #  This just returns an estimate of which nodes  we expect
                #  the op to touch. We don't know exactly which ones they will
                #  be because auto-sharding could put shards anywhere...
                # TODO: We already do this in diskCost(). We should try to reuse the calculations
                map(nodeCounts.put, self.estimator.estimateOp(design, op))
                query_count += 1
        ## FOR

        if self.debug: LOG.debug("Node Count Histogram:\n%s", nodeCounts)
        total = nodeCounts.getSampleCount()
        if not total: return 0.0, query_count

        best = 1 / float(self.num_nodes)
        skew = 0.0
        for i in xrange(0, self.num_nodes):
            ratio = nodeCounts.get(i, 0) / float(total)
            if ratio < best:
                ratio = best + ((1 - ratio/best) * (1 - best))
            skew += math.log(ratio / best)
        return skew / (math.log(1 / best) * self.num_nodes), query_count
    ## DEF

    ## -----------------------------------------------------------------------
    ## NETWORK COST
    ## -----------------------------------------------------------------------

    def networkCost(self, design):
        if self.debug: LOG.debug("Computing network cost for %d sessions", len(self.workload))
        result = 0
        query_count = 0
        for sess in self.workload:
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
                    result += len(self.estimator.estimateOp(design, op))
                else:
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s [parent=%s / previous=%s]", \
                                             op['type'], op['query_id'], op['collection'], \
                                             parent_col, (previous_op != None))
                ## IF
                previous_op = op
        if not query_count:
            cost = 0
        else:
            cost = result / float(query_count * self.num_nodes)

        LOG.info("Computed Network Cost: %f [result=%d / queryCount=%d]", \
                 cost, result, query_count)

        return cost
    ## DEF

    ## -----------------------------------------------------------------------
    ## WORKLOAD SEGMENTATION
    ## -----------------------------------------------------------------------

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

        if self.debug:
            LOG.debug("Workload Segments - START:%d / END:%d", start_time, end_time)
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
## CLASS