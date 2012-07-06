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

import workload
from nodeestimator import NodeEstimator
from util import constants
from util import Histogram
import catalog
import disk
import skew
import network
from abstractcostcomponent import AbstractCostComponent

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
        self.last_design = None
        self.last_cost = None



        self.estimator = NodeEstimator(self.collections, self.num_nodes)

        ## ----------------------------------------------
        ## COST COMPONENTS
        ## ----------------------------------------------
        self.diskComponent = disk.DiskCostComponent(self)
        self.skewComponent = skew.SkewCostComponent(self)
        self.networkComponent = network.NetworkCostComponent(self)

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

    def reset(self):
        """
            Reset all of the internal state and cache information
        """
        self.estimator.reset()

        map(AbstractCostComponent.reset, [self.diskComponent, self.skewComponent, self.networkComponent])

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

## CLASS