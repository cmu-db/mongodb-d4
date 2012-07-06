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
import math

# mongodb-d4
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))
from costmodel import AbstractCostComponent

from util import Histogram

LOG = logging.getLogger(__name__)

## ==============================================
## Skew Cost
## ==============================================
class SkewCostComponent(AbstractCostComponent):

    def __init__(self, costModel):
        AbstractCostComponent.__init__(self, costModel)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.workload_segments = [ ]

        # Pre-split the workload into separate intervals
        self.splitWorkload()
    ## DEF

    def getCostImpl(self, design):
        """Calculate the network cost for each segment for skew analysis"""

        # If there is only one node, then the cost is always zero
        if self.cm.num_nodes == 1:
            return 0.0

        op_counts = [ 0 ] *  self.cm.skew_segments
        segment_skew = [ 0 ] *  self.cm.skew_segments
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
            LOG.debug("Computing skew cost for %d sessions over %d segments", len(segment), self.cm.skew_segments)

        # Check whether we already have a histogram of how often each of the
        # nodes are touched from the NodeEstimator. This will have been computed
        # in diskCost()
        if not self.cm.estimator.getOpCount():
            # Iterate over each session and get the list of nodes
            # that we estimate that each of its operations will need to touch
            for sess in segment:
                for op in sess['operations']:
                    # Skip anything that doesn't have a design configuration
                    if not design.hasCollection(op['collection']):
                        if self.debug:
                            LOG.debug("SKIP - %s Op #%d on %s", op['type'], op['query_id'], op['collection'])
                        continue
                    col_info = self.cm.collections[op['collection']]
                    cache = self.cm.getCacheHandle(col_info)

                    #  This just returns an estimate of which nodes  we expect
                    #  the op to touch. We don't know exactly which ones they will
                    #  be because auto-sharding could put shards anywhere...
                    node_ids = self.cm.__getNodeIds__(cache, design, op)
                    # TODO: Do something with the nodeIds. Don't rely on the NodeEstimator's
                    #       internal histogram
                ## FOR

        if self.debug: LOG.debug("Node Count Histogram:\n%s", self.cm.estimator.nodeCounts)
        total = self.cm.estimator.nodeCounts.getSampleCount()
        if not total: return 0.0, self.cm.estimator.getOpCount()

        best = 1 / float(self.cm.num_nodes)
        skew = 0.0
        for i in xrange(self.cm.num_nodes):
            ratio = self.cm.estimator.nodeCounts.get(i, 0) / float(total)
            if ratio < best:
                ratio = best + ((1 - ratio/best) * (1 - best))
            skew += math.log(ratio / best)
        return skew / (math.log(1 / best) * self.cm.num_nodes), self.cm.estimator.getOpCount()
    ## DEF

        ## -----------------------------------------------------------------------
    ## WORKLOAD SEGMENTATION
    ## -----------------------------------------------------------------------

    def splitWorkload(self):
        """Divide the workload up into segments for skew analysis"""

        start_time = None
        end_time = None
        for i in xrange(len(self.cm.workload)):
            if start_time is None or start_time < self.cm.workload[i]['start_time']:
                start_time = self.cm.workload[i]['start_time']
            if end_time is None or end_time > self.cm.workload[i]['end_time']:
                end_time = self.cm.workload[i]['end_time']
        assert start_time,\
            "Failed to find start time in %d sessions" % len(self.cm.workload)
        assert end_time,\
            "Failed to find end time in %d sessions" % len(self.cm.workload)

        if self.debug:
            LOG.debug("Workload Segments - START:%d / END:%d", start_time, end_time)
        self.workload_segments = [ [] for i in xrange(0, self.cm.skew_segments) ]
        segment_h = Histogram()
        for sess in self.cm.workload:
            idx = self.getSessionSegment(sess, start_time, end_time)
            segment_h.put(idx)
            assert idx >= 0 and idx < self.cm.skew_segments,\
                "Invalid workload segment '%d' for Session #%d\n%s" % (idx, sess['session_id'], segment_h)
            self.workload_segments[idx].append(sess)
        ## FOR
    ## DEF

    def getSessionSegment(self, sess, start_time, end_time):
        """Return the segment offset that the given Session should be assigned to"""
        timestamp = sess['start_time']
        if timestamp == end_time: timestamp -= 1
        ratio = (timestamp - start_time) / float(end_time - start_time)
        return min(self.cm.skew_segments-1, int(self.cm.skew_segments * ratio)) # HACK
    ## DEF
## CLASS