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
from costmodel import disk
from costmodel.abstractcostcomponent import AbstractCostComponent

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
        """
            Internal cache for a single collection.
            Note that this is different than the LRUBuffer cache stuff. These are
            cached look-ups that the CostModel uses for figuring out what operations do.
        """

        def __init__(self, col_info, num_nodes):

            # The number of pages needed to do a full scan of this collection
            # The worst case for all other operations is if we have to do
            # a full scan that requires us to evict the entire buffer
            # Hence, we multiple the max pages by two
#            self.fullscan_pages = (col_info['max_pages'] * 2)
            self.fullscan_pages = (col_info['doc_count'] * 2)
            assert self.fullscan_pages > 0, \
                "Zero max_pages for collection '%s'" % col_info['name']

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
            lru = LRUBuffer(self.collections, self.max_memory, preload=True) # constants.DEFAULT_LRU_PRELOAD)
            self.buffers.append(lru)


        ## ----------------------------------------------
        ## COST COMPONENTS
        ## ----------------------------------------------
        self.diskComponent = disk.
        self.skewComponent = None
        self.networkComponent = None

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
        map(self.invalidateCache, design.getDelta(self.last_design))

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

        map(AbstractCostComponent.finish, [self.diskComponent, self.skewComponent, self.networkComponent])

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
        self.diskComponent.getCost(design)
    ## DEF



    ## -----------------------------------------------------------------------
    ## SKEW COST
    ## -----------------------------------------------------------------------

    def skewCost(self, design):
        return self.skewComponent.getCost(design)

    ## -----------------------------------------------------------------------
    ## NETWORK COST
    ## -----------------------------------------------------------------------

    def networkCost(self, design):
        return self.networkComponent.getCost(design)
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