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

# mongodb-d4
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

from costmodel import AbstractCostComponent
from workload import Session
from util import Histogram, constants

LOG = logging.getLogger(__name__)

## ==============================================
## Network Cost
## ==============================================
class NetworkCostComponent(AbstractCostComponent):

    def __init__(self, state):
        AbstractCostComponent.__init__(self, state)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def getCostImpl(self, design):
        if self.debug: LOG.debug("Computing network cost for %d sessions", len(self.state.workload))
        
        # TODO: Build a cache for the network cost per collection
        #       That way if the design doesn't change for a collection, we
        #       can reuse the message & query counts from the last calculation
        msg_count = 0
        op_count = 0
        for sess in self.state.workload:
            for op in sess['operations']:
                # Collection is not in design.. don't count query
                if not design.hasCollection(op['collection']):
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s",\
                                             op['type'], op['query_id'], op['collection'])
                    continue
                
                # Process this op!
                cache = self.state.getCacheHandleByName(op['collection'])
                op_count += 1
                msg_count += len(self.state.__getNodeIds__(cache, design, op))
        if not op_count:
            cost = 0
        else:
            cost = msg_count / float(op_count * self.state.num_nodes)

        if self.debug: LOG.debug("Computed Network Cost: %f [msgCount=%d / opCount=%d]",\
                                 cost, msg_count, op_count)
        return cost
    ## DEF
## CLASS