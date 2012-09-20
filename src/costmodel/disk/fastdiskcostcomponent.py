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
import os
import sys
import math
import logging
from pprint import pformat

# mongodb-d4
import workload

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

import catalog
from costmodel import AbstractCostComponent
from lrubuffer import LRUBuffer
from workload import Session
from util import Histogram, constants

LOG = logging.getLogger(__name__)

## ==============================================
## Faster Disk Cost Calculator
## ==============================================
class FastDiskCostComponent(AbstractCostComponent):

    def __init__(self, state):
        AbstractCostComponent.__init__(self, state)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.buffers = [ ]
        for i in xrange(self.state.num_nodes):
            lru = LRUBuffer(self.state.collections, self.state.max_memory, preload=constants.DEFAULT_LRU_PRELOAD)
            self.buffers.append(lru)
    ## DEF

    def getCostImpl(self, design):
        """
        """
        # Initialize all of the LRU buffers
        delta = self.__getDelta__(design)

        for lru in self.buffers:
            lru.initialize(design, delta)
            LOG.info(lru)
            lru.validate()


        final_cost = 0.0

        return final_cost
    ## DEF

    def __getDelta__(self, design):
        """
            We calculate delta here so that it won't be executed
            in every lru. All lrus will share the same delta value for it's
            only related to the design and collections
        """
        percent_total = 0

        for col_name in design.getCollections():
            col_info = self.state.collections[col_name]
            percent_total += col_info['workload_percent']

        assert 0.0 < percent_total <= 1.0, "Invalid workload percent total %f" % percent_total

        return 1.0 + ((1.0 - percent_total) / percent_total)

    def finish(self):
        pass
    ## DEF

    def reset(self):
        pass
    ## DEF


## CLASS