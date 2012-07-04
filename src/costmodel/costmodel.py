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
import time
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

    class Cache():
        def __init__(self, col_info, num_nodes):

            # The number of pages needed to do a full scan of this collection
            # The worst case for all other operations is if we have to do
            # a full scan that requires us to evict the entire buffer
            # Hence, we multiple the max pages by two
            self.fullscan_pages = (col_info['max_pages'] * 2) # / num_nodes

            # Cache of Best Index Tuples
            # QueryHash -> BestIndex
            self.best_index = { }

            # Cache of Regex Operations
            # QueryHash -> Boolean
            self.op_regex = { }

            # Cache of Touched Node Ids
            # QueryId -> [NodeId]
            self.op_nodeIds = { }

            # Cache of Document Ids
            # QueryId -> Index/Collection DocumentIds
            self.collection_docIds = { }
            self.index_docIds = { }
        ## DEF

        def reset(self):
            self.best_index.clear()
            self.op_regex.clear()
            self.op_nodeIds.clear()
            self.collection_docIds.clear()
            self.index_docIds.clear()
        ## DEF

        def __str__(self):
            ret = ""
            max_len = max(map(len, self.__dict__.iterkeys()))+1
            f = "  %-" + str(max_len) + "s %s\n"
            for k,v in self.__dict__.iteritems():
                if isinstance(v, dict):
                    v_str = "[%d entries]" % len(v)
                else:
                    v_str = str(v)
                ret += f % (k+":", v_str)
            return ret
        ## DEF
    ## CLASS

    def __init__(self, collections, workload, config):
        assert isinstance(collections, dict)
#        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.collections = collections
        self.workload = workload

        self.weight_network = config.get('weight_network', 1.0)
        self.weight_disk = config.get('weight_disk', 1.0)
        self.weight_skew = config.get('weight_skew', 1.0)
        self.num_nodes = config.get('nodes', 1)

        self.last_design = None
        self.last_cost = None

        # Convert MB to bytes
        self.max_memory = config['max_memory'] * 1024 * 1024
        self.skew_segments = config['skew_intervals'] # Why? "- 1"
        self.address_size = config['address_size'] / 4

        self.estimator = NodeEstimator(self.collections, self.num_nodes)
        self.buffers = [ ]
        for i in xrange(self.num_nodes):
            lru = LRUBuffer(self.collections, self.max_memory)
            self.buffers.append(lru)
        ## ----------------------------------------------
        ## CACHING
        ## ----------------------------------------------
        self.cache_enable = True
        self.cache_miss_ctr = Histogram()
        self.cache_hit_ctr = Histogram()

        # ColName -> CacheHandle
        self.cache_handles = { }

        ## ----------------------------------------------
        ## PREP
        ## ----------------------------------------------

        # Pre-split the workload into separate intervals
        self.splitWorkload()
    ## DEF

    def overallCost(self, design):

        # TODO: We should reset any cache entries for only those collections
        #       that were changed in this new design from the last design
        delta = design.getDelta(self.last_design)
        map(self.invalidateCache, delta)

        if self.debug:
            LOG.debug("New Design:\n%s", design)
            self.cache_hit_ctr.clear()
            self.cache_miss_ctr.clear()
        start = time.time()

        cost = 0.0
        cost += self.weight_disk * self.diskCost(design)
        cost += self.weight_network * self.networkCost(design)
        cost += self.weight_skew * self.skewCost(design)

        self.last_cost = cost / float(self.weight_network + self.weight_disk + self.weight_skew)
        self.last_design = design

#        if self.debug:
        stop = time.time()

        # Calculate cache hit/miss ratio
        LOG.info("Overall Cost %.3f / Computed in %.2f seconds", \
                 self.last_cost, (stop - start))

        buffer_total = sum([ lru.buffer_size for lru in self.buffers ])
        buffer_remaining = sum([ lru.remaining for lru in self.buffers ])
        buffer_ratio = (buffer_total - buffer_remaining) / float(buffer_total)
        LOG.info("Buffer Usage %.2f%% [total=%d / used=%d]", \
                  buffer_ratio*100, buffer_total, (buffer_total - buffer_remaining))

        if self.debug:
            cache_success = sum([ x for x in self.cache_hit_ctr.itervalues() ])
            cache_miss = sum([ x for x in self.cache_miss_ctr.itervalues() ])
            cache_ratio = cache_success / float(cache_success + cache_miss)
            LOG.debug("Internal Cache Ratio %.2f%% [total=%d]", cache_ratio*100, (cache_miss+cache_success))
            LOG.debug("Cache Hits [%d]:\n%s", cache_success, self.cache_hit_ctr)
            LOG.debug("Cache Misses [%d]:\n%s", cache_miss, self.cache_miss_ctr)
            LOG.debug("-"*100)

        return self.last_cost
    ## DEF

    def invalidateCache(self, col_name):
        if col_name in self.cache_handles:
            if self.debug: LOG.debug("Invalidating cache for collection '%s'", col_name)
            self.cache_handles[col_name].reset()
    ## DEF

    def getCacheHandle(self, col_info):
        cache = self.cache_handles.get(col_info['name'], None)
        if cache is None:
            cache = CostModel.Cache(col_info, self.num_nodes)
            self.cache_handles[col_info['name']] = cache
        return cache
    ## DEF

    def reset(self):
        """
            Reset all of the internal state and cache information
        """
        # Clear out caches for all collections
        self.cache_handles.clear()
        self.estimator.reset()

        for lru in self.buffers:
            lru.reset()
    ## DEF

    ## -----------------------------------------------------------------------
    ## DISK COST
    ## -----------------------------------------------------------------------

    def diskCost(self, design):
        """
            Estimate the Disk Cost for a design and a workload
            Note: If this is being invoked with overallCost(), then the diskCost()
            should be calculated before skewCost() because we will reused the same
            histogram of how often nodes are touched in the workload
        """

        num_sessions = len(self.workload)
#        if self.debug:
        LOG.info("Calculating diskCost for %d sessions", num_sessions)

        # Initialize all of the LRU buffers
        for lru in self.buffers:
            lru.initialize(design)

        # Ok strap on your helmet, this is the magical part of the whole thing!
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

        # Worst case is when every query requires a full collection scan
        # Best case, every query is satisfied by main memory
        totalWorst = 0
        totalCost = 0
        sess_ctr = 0
        for sess in self.workload:
            for op in sess['operations']:
                # is the collection in the design - if not ignore
                if not design.hasCollection(op['collection']):
                    continue
                col_info = self.collections[op['collection']]

                # Initialize cache if necessary
                # We will always want to do this regardless of whether caching is enabled
                cache = self.getCacheHandle(col_info)

                # Check whether we have a cache index selection based on query_hashes
                indexKeys, covering = cache.best_index.get(op["query_hash"], (None, None))
                if indexKeys is None:
                    indexKeys, covering = self.guessIndex(design, op)
                    if self.cache_enable:
                        if self.debug: self.cache_miss_ctr.put("best_index")
                        cache.best_index[op["query_hash"]] = (indexKeys, covering)
                elif self.debug:
                    self.cache_hit_ctr.put("best_index")
                pageHits = 0
                maxHits = 0
                isRegex = self.__getIsOpRegex__(cache, op)

                # Grab all of the query contents
                for content in workload.getOpContents(op):
#                    print "-"*50
#                    print pformat(content)

                    for node_id in self.__getNodeIds__(cache, design, op):
                        lru = self.buffers[node_id]

                        # TODO: Need to handle whether it's a scan or an equality predicate
                        # TODO: We need to handle when we have a regex predicate. These are tricky
                        #       because they may use an index that will examine all a subset of collections
                        #       and then execute a regex on just those documents.

                        # If we have a target index, hit that up
                        if indexKeys and not isRegex: # FIXME
                            documentId = cache.index_docIds.get(op['query_id'], None)
                            if documentId is None:
                                values = catalog.getFieldValues(indexKeys, content)
                                try:
                                    documentId = hash(values)
                                except:
                                    LOG.error("Failed to compute index documentIds for op #%d - %s\n%s", \
                                              op['query_id'], values, pformat(op))
                                    raise
                                if self.cache_enable:
                                    if self.debug: self.cache_miss_ctr.put("index_docIds")
                                    cache.index_docIds[op['query_id']] = documentId
                            elif self.debug:
                                self.cache_hit_ctr.put("index_docIds")
                            ## IF
                            hits = lru.getDocumentFromIndex(op['collection'], indexKeys, documentId)
                            pageHits += hits
                            maxHits += hits if op['type'] == constants.OP_TYPE_INSERT else cache.fullscan_pages
                            if self.debug:
                                LOG.debug("Node #%02d: Estimated %d index scan pageHits for op #%d on %s.%s",\
                                          node_id, hits, op["query_id"], op["collection"], indexKeys)

                        # If we don't have an index, then we know that it's a full scan because the
                        # collections are unordered
                        if not indexKeys:
                            if self.debug:
                                LOG.debug("No index available for op #%d. Will have to do full scan on '%s'", \
                                          op["query_id"], op["collection"])
                            pageHits += cache.fullscan_pages
                            maxHits += cache.fullscan_pages

                        # Otherwise, if it's not a covering index, then we need to hit up
                        # the collection to retrieve the whole document
                        elif not covering:
                            documentId = cache.collection_docIds.get(op['query_id'], None)
                            if documentId is None:
                                values = catalog.getAllValues(content)
                                try:
                                    documentId = hash(values)
                                except:
                                    LOG.error("Failed to compute collection documentIds for op #%d - %s\n%s",\
                                              op['query_id'], values, pformat(op))
                                    raise
                                if self.cache_enable:
                                    if self.debug: self.cache_miss_ctr.put("collection_docIds")
                                    cache.collection_docIds[op['query_id']] = documentId
                            elif self.debug:
                                self.cache_hit_ctr.put("collection_docIds")
                            ## IF
                            hits = lru.getDocumentFromCollection(op['collection'], documentId)
                            pageHits += hits
                            maxHits += hits if op['type'] == constants.OP_TYPE_INSERT else cache.fullscan_pages
                            if self.debug:
                                LOG.debug("Node #%02d: Estimated %d collection scan pageHits for op #%d on %s",\
                                          node_id, hits, op["query_id"], op["collection"])
                    ## FOR (node)
                ## FOR (content)

                totalCost += pageHits
                totalWorst += maxHits
                if self.debug:
                    LOG.debug("Op #%d on '%s' -> [pageHits:%d / worst:%d]", \
                              op["query_id"], op["collection"], pageHits, maxHits)
                assert pageHits <= maxHits,\
                    "Estimated pageHits [%d] is greater than worst [%d] for op #%d\n%s" % (pageHits, maxHits, op["query_id"], pformat(op))
        ## FOR (op)
            sess_ctr += 1
            if sess_ctr % 1000 == 0: LOG.info("Session %5d / %d", sess_ctr, num_sessions)
        ## FOR (sess)

        # The final disk cost is the ratio of our estimated disk access cost divided
        # by the worst possible cost for this design. If we don't have a worst case,
        # then the cost is simply zero
        assert totalCost <= totalWorst, \
            "Estimated total pageHits [%d] is greater than worst case pageHits [%d]" % (totalCost, totalWorst)
        final_cost = totalCost / totalWorst if totalWorst else 0
        evicted = sum([ lru.evicted for lru in self.buffers ])
        LOG.info("Computed Disk Cost: %.03f [pageHits=%d / worstCase=%d / evicted=%d]", \
                 final_cost, totalCost, totalWorst, evicted)
        return final_cost
    ## DEF

    def guessIndex(self, design, op):
        """
            Return a tuple containing the best index to use for this operation and a boolean
            flag that is true if that index covers the entire operation's query
        """

        # Simply choose the index that has most of the fields
        # referenced in the operation.
        indexes = design.getIndexes(op['collection'])
        op_contents = workload.getOpContents(op)
        best_index = None
        best_ratio = None
        for i in xrange(len(indexes)):
            field_cnt = 0
            for indexKey in indexes[i]:
                # We can't use a field if it's being used in a regex operation
                if catalog.hasField(indexKey, op_contents) and not workload.isOpRegex(op, field=indexKey):
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
        if self.debug:
            LOG.debug("Op #%d - BestIndex:%s / BestRatio:%s", \
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

        # If there is only one node, then the cost is always zero
        if self.num_nodes == 1:
            return 0.0

        op_counts = [ 0 ] *  self.skew_segments
        segment_skew = [ 0 ] *  self.skew_segments
        for i in range(0, len(self.workload_segments)):
            # TODO: We should cache this so that we don't have to call it twice
            segment_skew[i], op_counts[i] = self.calculateSkew(design, self.workload_segments[i])

        weighted_skew = sum([segment_skew[i] * op_counts[i] for i in xrange(len(self.workload_segments))])
        cost = weighted_skew / float(sum(op_counts))
        LOG.info("Computed Skew Cost: %f", cost)
        return cost
    ## DEF

    def calculateSkew(self, design, segment):
        """
            Calculate the cluster skew factor for the given workload segment
            See Alg.#3 from Pavlo et al. 2012:
            http://hstore.cs.brown.edu/papers/hstore-partitioning.pdf
        """
        if self.debug:
            LOG.debug("Computing skew cost for %d sessions over %d segments", len(segment), self.skew_segments)

        # Check whether we already have a histogram of how often each of the
        # nodes are touched from the NodeEstimator. This will have been computed
        # in diskCost()
        if not self.estimator.getOpCount():
            # Iterate over each session and get the list of nodes
            # that we estimate that each of its operations will need to touch
            for sess in segment:
                for op in sess['operations']:
                    # Skip anything that doesn't have a design configuration
                    if not design.hasCollection(op['collection']):
                        if self.debug:
                            LOG.debug("SKIP - %s Op #%d on %s", op['type'], op['query_id'], op['collection'])
                        continue
                    col_info = self.collections[op['collection']]
                    cache = self.getCacheHandle(col_info)

                    #  This just returns an estimate of which nodes  we expect
                    #  the op to touch. We don't know exactly which ones they will
                    #  be because auto-sharding could put shards anywhere...
                    node_ids = self.__getNodeIds__(cache, design, op)
                    # TODO: Do something with the nodeIds. Don't rely on the NodeEstimator's
                    #       internal histogram
            ## FOR

        if self.debug: LOG.debug("Node Count Histogram:\n%s", self.estimator.nodeCounts)
        total = self.estimator.nodeCounts.getSampleCount()
        if not total: return 0.0, self.estimator.getOpCount()

        best = 1 / float(self.num_nodes)
        skew = 0.0
        for i in xrange(self.num_nodes):
            ratio = self.estimator.nodeCounts.get(i, 0) / float(total)
            if ratio < best:
                ratio = best + ((1 - ratio/best) * (1 - best))
            skew += math.log(ratio / best)
        return skew / (math.log(1 / best) * self.num_nodes), self.estimator.getOpCount()
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
                # Collection is not in design.. don't count query
                if not design.hasCollection(op['collection']):
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s", \
                                             op['type'], op['query_id'], op['collection'])
                    continue
                col_info = self.collections[op['collection']]
                cache = self.getCacheHandle(col_info)

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
                    result += len(self.__getNodeIds__(cache, design, op))
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
    ## UTILITY CODE
    ## -----------------------------------------------------------------------

    def __getIsOpRegex__(self, cache, op):
        isRegex = cache.op_regex.get(op["query_hash"], None)
        if isRegex is None:
            isRegex = workload.isOpRegex(op)
            if self.cache_enable:
                if self.debug: self.cache_miss_ctr.put("op_regex")
                cache.op_regex[op["query_hash"]] = isRegex
        elif self.debug:
            self.cache_hit_ctr.put("op_regex")
        return isRegex
    ## DEF


    def __getNodeIds__(self, cache, design, op):
        node_ids = cache.op_nodeIds.get(op['query_id'], None)
        if node_ids is None:
            try:
                node_ids = self.estimator.estimateNodes(design, op)
            except:
                LOG.error("Failed to estimate touched nodes for op #%d\n%s", op['query_id'], pformat(op))
                raise
            if self.cache_enable:
                if self.debug: self.cache_miss_ctr.put("op_nodeIds")
                cache.op_nodeIds[op['query_id']] = node_ids
            if self.debug:
                LOG.debug("Estimated Touched Nodes for Op #%d: %d", op['query_id'], len(node_ids))
        elif self.debug:
            self.cache_hit_ctr.put("op_nodeIds")
        return node_ids
    ## DEF


    ## -----------------------------------------------------------------------
    ## WORKLOAD SEGMENTATION
    ## -----------------------------------------------------------------------

    def splitWorkload(self):
        """Divide the workload up into segments for skew analysis"""

        start_time = None
        end_time = None
        for i in xrange(len(self.workload)):
            if start_time is None or start_time < self.workload[i]['start_time']:
                start_time = self.workload[i]['start_time']
            if end_time is None or end_time > self.workload[i]['end_time']:
                end_time = self.workload[i]['end_time']
        assert start_time, \
            "Failed to find start time in %d sessions" % len(self.workload)
        assert end_time, \
            "Failed to find end time in %d sessions" % len(self.workload)

        if self.debug:
            LOG.debug("Workload Segments - START:%d / END:%d", start_time, end_time)
        self.workload_segments = [ [] for i in xrange(0, self.skew_segments) ]
        segment_h = Histogram()
        for sess in self.workload:
            idx = self.getSessionSegment(sess, start_time, end_time)
            segment_h.put(idx)
            assert idx >= 0 and idx < self.skew_segments, \
                "Invalid workload segment '%d' for Session #%d\n%s" % (idx, sess['session_id'], segment_h)
            self.workload_segments[idx].append(sess)
        ## FOR
    ## DEF

    def getSessionSegment(self, sess, start_time, end_time):
        """Return the segment offset that the given Session should be assigned to"""
        timestamp = sess['start_time']
        if timestamp == end_time: timestamp -= 1
        ratio = (timestamp - start_time) / float(end_time - start_time)
        return min(self.skew_segments-1, int(self.skew_segments * ratio)) # HACK
    ## DEF
## CLASS