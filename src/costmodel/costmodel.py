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
from state import State
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
        self.state = State(collections, workload, config)

        self.weights_sum = 0
        for k, v in self.state.__dict__.iteritems():
            if k.startswith("weight_"): self.weights_sum += v

        ## ----------------------------------------------
        ## COST COMPONENTS
        ## ----------------------------------------------
        self.diskComponent = disk.FastDiskCostComponent(self.state)
        self.skewComponent = skew.SkewCostComponent(self.state)
        self.networkComponent = network.NetworkCostComponent(self.state)
        self.allComponents = (self.diskComponent, self.skewComponent, self.networkComponent)
        
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def overallCost(self, design):

        # TODO: We should reset any cache entries for only those collections
        #       that were changed in this new design from the last design
        map(self.invalidateCache, design.getDelta(self.last_design))

        if self.debug:
            LOG.debug("New Design:\n%s", design)
            self.state.cache_hit_ctr.clear()
            self.state.cache_miss_ctr.clear()
        start = time.time()

        cost = 0.0
        cost += self.state.weight_disk * self.diskComponent.getCost(design)
        cost += self.state.weight_network * self.networkComponent.getCost(design)
        cost += self.state.weight_skew * self.skewComponent.getCost(design)

        self.last_cost = cost / float(self.weights_sum)
        self.last_design = design

#        if self.debug:
        stop = time.time()

        # Calculate cache hit/miss ratio
        LOG.info("Overall Cost %.3f / Computed in %.2f seconds", \
                 self.last_cost, (stop - start))

        map(AbstractCostComponent.finish, self.allComponents)
        return self.last_cost
    ## DEF

    def invalidateCache(self, col_name):
        self.state.invalidateCache(col_name)
        for c in self.allComponents:
            c.invalidateCache(col_name)
    ## DEF

    def reset(self):
        """Reset all of the internal state and cache information"""
        self.state.reset()
        map(AbstractCostComponent.reset, self.allComponents)
    ## DEF
## CLASS