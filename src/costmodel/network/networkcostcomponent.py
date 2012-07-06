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
import logging

# mongodb-d4
import math

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))
from costmodel import AbstractCostComponent

import parser
import reconstructor
import sessionizer
from workload import Session
from util import Histogram, constants

LOG = logging.getLogger(__name__)

## ==============================================
## Network Cost
## ==============================================
class NetworkCostComponent(AbstractCostComponent):

    def __init__(self, costModel):
        AbstractCostComponent.__init__(self, costModel)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def getCostImpl(self, design):
        if self.debug: LOG.debug("Computing network cost for %d sessions", len(self.cm.workload))
        result = 0
        query_count = 0
        for sess in self.cm.workload:
            previous_op = None
            for op in sess['operations']:
                # Collection is not in design.. don't count query
                if not design.hasCollection(op['collection']):
                    if self.debug: LOG.debug("SKIP - %s Op #%d on %s",\
                                             op['type'], op['query_id'], op['collection'])
                    continue
                col_info = self.cm.collections[op['collection']]
                cache = self.cm.getCacheHandle(col_info)

                # Check whether this collection is embedded inside of another
                # TODO: Need to get ancestor
                parent_col = design.getDenormalizationParent(op['collection'])
                if self.debug and parent_col:
                    LOG.debug("Op #%d on '%s' Parent Collection -> '%s'",\
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
                    result += len(self.cm.__getNodeIds__(cache, design, op))
                elif self.debug:
                    LOG.debug("SKIP - %s Op #%d on %s [parent=%s / previous=%s]",\
                        op['type'], op['query_id'], op['collection'],\
                        parent_col, (previous_op != None))
                ## IF
                previous_op = op
        if not query_count:
            cost = 0
        else:
            cost = result / float(query_count * self.cm.num_nodes)

        LOG.info("Computed Network Cost: %f [result=%d / queryCount=%d]",\
            cost, result, query_count)

        return cost
    ## DEF
## CLASS


