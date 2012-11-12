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
        
        # COL_NAME -> [OP_COUNT, MSG_COUNT]
        self.cache = { }
        self.lastDesign = None
        
        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def invalidateCache(self, newDesign, col_name):
        # Check whether the denormalization scheme or sharding keys have changed
        if newDesign.hasDenormalizationChanged(self.lastDesign, col_name) or \
           newDesign.hasShardingKeysChanged(self.lastDesign, col_name):
            if col_name in self.cache: del self.cache[col_name]
    ## DEF

    def reset(self):
        self.cache = { }

    def getCostImpl(self, design):
        if self.debug:
            LOG.debug("Computing network cost for %d sessions [origOpCount=%d / numNodes=%d]", len(self.state.workload), self.state.orig_op_count, self.state.num_nodes)
        self.lastDesign = design
        
        # Build a cache for the network cost per collection
        # That way if the design doesn't change for a collection, we
        # can reuse the message & op counts from the last calculation
        cost = 0
        total_op_count = 0
        total_msg_count = 0
        for col_name in self.state.collections.iterkeys():
            # Collection is not in design.. don't include the op
            if not design.hasCollection(col_name):
                if self.debug: LOG.debug("NOT in design: SKIP - All operations on %s", col_name)
                continue
            if design.isRelaxed(col_name):
                if self.debug: LOG.debug("Relaxed: SKIP - All operations on %s", col_name)
                continue
            
            if col_name in self.cache:
                total_op_count += self.cache[col_name][0]
                total_msg_count += self.cache[col_name][1]
            else:
                # TODO: The operations should come from the state handle, which
                #       will have already combined things for us based on the design
                op_count = 0
                msg_count = 0
                for op in self.state.col_op_xref[col_name]:
                    # Process this op!
                    cache = self.state.getCacheHandleByName(col_info = self.state.collections[col_name])
                    op_count += 1
                    msgs = self.state.__getNodeIds__(cache, design, op)
                    assert len(msgs) <= self.state.num_nodes, \
                        "%s -- NumMsgs[%d] <= NumNodes[%d]" % (msgs, len(msgs), self.state.num_nodes)
                    msg_count += len(msgs)
                    # if self.debug: LOG.debug("%s -> Messages %s", op, msgs)
                
                # Store it in our cache so that we can reuse it
                self.cache[col_name] = (op_count, msg_count)

                total_op_count += op_count
                total_msg_count += msg_count

        if total_op_count > 0:
            cost = total_msg_count / float(self.state.orig_op_count * self.state.num_nodes)

        if self.debug:
            LOG.debug("Computed Network Cost: %f [msgCount=%d / opCount=%d]",\
                      cost, total_msg_count, total_op_count)
        return cost
    ## DEF
## CLASS