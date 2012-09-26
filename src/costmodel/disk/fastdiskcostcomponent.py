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
        self.window_size = constants.WINDOW_SIZE

        # op -> flag, which tells if the operation exists in the previous table
        self.window = { }

        # initialize collections array
        # extract all the collections that will be touched
        # from workload and put them in an array
        self.collection_array = [ ]

        for sess in self.state.workload:
            for op in sess['operations']:
                self.collection_array.append(op['collection'])

    ## DEF

    def getCostImpl(self, design):

        totalWorst = 0
        totalCost = 0

        cursor = 0
        while cursor + self.window_size < len(self.collection_array):
            pass
        for col in self.collection_array:

            col_info = self.state.collections[col]

            cache = self.state.getCacheHandle(col_info)

            indexKeys, covering = cache.best_index.get(op['query_hash'], (None, None))
            if indexKeys is None:
                # ignore indexes so far
                pass
            elif self.debug:
                self.state.cache_hit_ctr.put("best_index")

            pageHits = 0
            maxHits = 0
            isRegex = self.state.__getIsOpRegex__(cache, op)

            # Grab all of the query contents
            for content in workload.getOpContents(op):
                for node_id in self.state.__getNodeIds__(cache, design, op):
                    if indexKeys and not isRegex:
                        # ignore indexes so far
                        pass
                    if not indexKeys:
                        pageHits += cache.fullscan_pages
                        maxHits += cache.fullscan_pages
                    elif not covering:
                        # calculate the hits using the window
                        pageHits += hits

            totalCost += pageHits
            totalWorst += maxHits

        final_cost = totalCost / totalWorst if totalWorst else 0

        return final_cost
    ## DEF

    def __getHits__(self, collections):
        hits = 0
        newWindow = { }

        for col in collections:
            newWindow[col] = True
            if col not in self.window:
                hits += 1

        # Update window
        self.window = newWindow

        return hits

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
