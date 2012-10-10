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
import logging
from pprint import pformat
import copy

# mongodb-d4
import workload
from nodeestimator import NodeEstimator
from util.histogram import Histogram

LOG = logging.getLogger(__name__)

class State():
    """Cost Model State"""

    ## -----------------------------------------------------------------------
    ## INTERNAL CACHE STATE
    ## -----------------------------------------------------------------------

    class Cache():
        """
            Internal cache for a single collection.
            Note that this is different than the LRUBuffer cache stuff. These are
            cached look-ups that the CostModel uses for figuring out what operations do.
        """
        def __init__(self, col_name, num_nodes):

            # The number of pages needed to do a full scan of this collection
            # The worst case for all other operations is if we have to do
            # a full scan that requires us to evict the entire buffer
            # Hence, we multiple the max pages by two
            # self.fullscan_pages = (col_info['max_pages'] * 2)
            self.fullscan_pages = 10 # FIXME(yang) (col_info['doc_count'] * 2)
            assert self.fullscan_pages > 0,\
                "Zero max_pages for collection '%s'" % col_name

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
            self.op_count = 0
            self.msg_count = 0
            self.network_reset = True
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

        # Convert MB to bytes
        self.max_memory = config['max_memory'] * 1024 * 1024
        self.skew_segments = config['skew_intervals'] # Why? "- 1"
        self.address_size = config['address_size'] / 4

        self.estimator = NodeEstimator(self.collections, self.num_nodes)
        
        self.window_size = config['window_size']

        # Build indexes from collections to sessions/operations
        # Note that this won't change dynamically based on denormalization schemes
        # It's up to the cost components to figure things out based on that
        self.col_sess_xref = dict([(col_name, []) for col_name in self.collections])
        self.col_op_xref = dict([(col_name, []) for col_name in self.collections])
        
        self.__buildCrossReference__();

        ## ----------------------------------------------
        ## CACHING
        ## ----------------------------------------------
        self.cache_enable = True
        self.cache_miss_ctr = Histogram()
        self.cache_hit_ctr = Histogram()

        # ColName -> CacheHandle
        self.cache_handles = { }

        self.originalWorload = None
        self.originalWorload = copy.deepcopy(self.workload)
        self.originalCollections = copy.deepcopy(self.collections)
        
    ## DEF

    def updateWorkload(self, workload):
        self.workload = workload
        self.col_sess_xref = dict([(col_name, []) for col_name in self.collections])
        self.col_op_xref = dict([(col_name, []) for col_name in self.collections])
        self.__buildCrossReference__()

    def restoreOriginalWorkload(self):
        self.workload = self.originalWorload
        self.collections = self.originalCollections
        
        self.col_sess_xref = dict([(col_name, []) for col_name in self.collections])
        self.col_op_xref = dict([(col_name, []) for col_name in self.collections])        
        self.__buildCrossReference__()

    def __buildCrossReference__(self):
        for sess in self.workload:
            cols = set()
            for op in sess["operations"]:
                col_name = op["collection"]
                if col_name in self.col_sess_xref:
                    self.col_op_xref[col_name].append(op)
                    cols.add(col_name)
            ## FOR (op)
            for col_name in cols:
                self.col_sess_xref[col_name].append(sess)
        ## FOR (sess)
        
    def invalidateCache(self, col_name):
        if col_name in self.cache_handles:
            if self.debug: LOG.debug("Invalidating cache for collection '%s'", col_name)
            self.cache_handles[col_name].reset()
    ## DEF

    def getCacheHandleByName(self, col_name):
        """
            Return a cache handle for the given collection name.
            This is the preferrred method because it requires fewer hashes
        """
        cache = self.cache_handles.get(col_name, None)
        if cache is None:
            cache = State.Cache(col_name, self.num_nodes)
            self.cache_handles[col_name] = cache
        return cache
    ## DEF
    
    def getCacheHandle(self, col_info):
        return self.getCacheHandleByName(col_info['name'])
    ## DEF

    def reset(self):
        """
            Reset all of the internal state and cache information
        """
        # Clear out caches for all collections
        self.cache_handles.clear()
        self.estimator.reset()

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

## CLASS