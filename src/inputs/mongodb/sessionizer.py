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
import random
import logging

from workload import Session
from util import Histogram
from util import mathutil

LOG = logging.getLogger(__name__)

RANDOMIZE_TARGET = 5

class Sessionizer:
    """Takes a series of operations and figures out session boundaries"""
    
    def __init__(self, metadata_db, randomize=False):
        self.metadata_db = metadata_db
        self.randomize = randomize
        self.op_ctr = 0
        
        # Reverse look-up for op hashes
        self.op_hash_xref = { }

        # Pairs of hashes that correspond to when one
        # session ends and the next one begins
        self.sessionBoundaries = set()
        
        # Mapping from SessionId -> Operations
        self.sessOps = { }
        
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def process(self, sessId, operations):
        """Process a list of operations from a single connection."""
        self.sessOps[sessId] = [ ]
        lastOp = None
        ctr = 0
        for op in sorted(operations, key=lambda op: op["query_time"]):
            # Calculate the difference between this op and the last
            if not "resp_time" in op: continue
            assert "query_hash" in op, \
                "Missing hash in operation %d" % op["query_id"]
            if op["resp_time"] is None: op["resp_time"] = op["query_time"] # HACK

            assert not op["query_time"] is None
            assert not op["resp_time"] is None

            if lastOp:
                # HACK
                if op["query_time"] < lastOp["resp_time"]:
                    if self.debug: 
                        LOG.warn("Op #%d query time (%d) comes before last op #%d response time (%d) in session #%d",
                                 op["query_id"], op["query_time"], lastOp["query_id"], lastOp["resp_time"], sessId)
                    temp = op["query_time"]
                    op["query_time"] = lastOp["resp_time"]
                    lastOp["resp_time"] = temp
                # Seconds -> Milliseconds
                if lastOp["resp_time"]:
                    diff = (op["query_time"]*1000) - (lastOp["resp_time"]*1000)
                    self.sessOps[sessId].append((lastOp, op, diff))
                ## IF
            lastOp = op
            ctr += 1
        ## FOR
        self.op_ctr += ctr
        LOG.debug("Examined %d operations for session %d [total=%d]" % (ctr, sessId, self.op_ctr))
    ## DEF
    
    def calculateSessions(self):
        # Calculate outliers using the quartile method
        # http://en.wikipedia.org/wiki/Quartile#Computing_methods
        if self.debug:
            LOG.debug("Calculating time difference for operations in %d sessions" % len(self.sessOps))
        
        # Get the full list of all the time differences
        allDiffs = [ ]
        for clientOps in self.sessOps.values():
            allDiffs += [x[-1] for x in clientOps]
        allDiffs = sorted(allDiffs)
        numDiffs = len(allDiffs)

        #print "\n".join(map(str, allDiffs))
        
        # Lower + Upper Quartiles
        lowerQuartile, upperQuartile = mathutil.quartiles(allDiffs)
        
        if lowerQuartile is None or upperQuartile is None:
            LOG.warn("Null quartiles! Can't continue!")
            return
        # Interquartile Range
        iqr = (upperQuartile - lowerQuartile) * 1.5
        
        if self.debug:
            LOG.debug("Calculating stats for %d op pairs" % len(allDiffs))
            LOG.debug("  Lower Quartile: %s" % lowerQuartile)
            LOG.debug("  Upper Quartile: %s" % upperQuartile)
            LOG.debug("  IQR: %s" % iqr)
        
        # Go through operations for each client and identify the
        # pairs of operations that are above the IQR in the upperQuartile
        opHist = Histogram()
        prevOpHist = Histogram()
        nextOpHist = Histogram()
        threshold = upperQuartile + iqr
        for sessId, clientOps in self.sessOps.iteritems():
            for op0, op1, opDiff in clientOps:
                if opDiff >= threshold:
                    prevOpHist.put(op0["query_hash"])
                    nextOpHist.put(op1["query_hash"])
                    opHist.put((op0["query_hash"], op1["query_hash"]))
            ## FOR
        ## FOR
        if self.debug:
            LOG.debug("Outlier Op Hashes:\n%s" % opHist)
        
        # I guess at this point we can just compute the outliers
        # again for the pairs of operations that have a time difference
        # outlier. We won't use the IQR. We'll just take the upper quartile
        # because that seems to give us the right answer
        outlierCounts = sorted(opHist.getCounts())
        lowerQuartile, upperQuartile = mathutil.quartiles(outlierCounts)
        
        if self.debug:
            LOG.debug("Calculating stats for %d count outliers" % len(outlierCounts))
            LOG.debug("  Lower Quartile: %s" % lowerQuartile)
            LOG.debug("  Upper Quartile: %s" % upperQuartile)
        
        self.sessionBoundaries.clear()
        
        # If we're doing this randomly, we want each session to have roughly 
        # the same number of operations as RANDOMIZE_TARGET
        if self.randomize:
            num_outliers = len(outlierCounts)
            force = 1 if int(num_outliers*0.10) == 1 else random.randint(1, int(num_outliers*0.10))
            LOG.warn("Forcing %d random outliers out of %d to be chosen from workload", force, num_outliers)
        else:
            force = 0
        for cnt in outlierCounts:
            if cnt >= upperQuartile or (self.randomize and force > 0):
                self.sessionBoundaries |= set(opHist.getValuesForCount(cnt))
                force -= 1
        ## FOR
        LOG.debug("Found %d outlier hashes" % len(self.sessionBoundaries))
    ## DEF
        
    def sessionize(self, origSess, nextId):
        """
            Once we have our session boundaries, we need to then loop through 
            each of the sessOps again and generate our boundaries.
        """
        sess = None
        newSessions = [ ]
        lastOp = None
        
        # Throw a None at the end of the list so that
        # we always loop through one extra time in order
        # to make sure the lastOp is given a home!
        for op in origSess["operations"] + [ None ]:
            if sess is None:
                sess = self.metadata_db.Session()
                sess["operations"] = [ ]
                sess["ip_client"] = origSess["ip_client"]
                sess["ip_server"] = origSess["ip_server"]
                sess["session_id"] = nextId
                nextId += 1
                newSessions.append(sess)
            if not lastOp is None:
                if len(sess["operations"]) == 0:
                    sess["start_time"] = lastOp["query_time"]
                sess["operations"].append(lastOp)
                if op and (lastOp["query_hash"], op["query_hash"]) in self.sessionBoundaries:
                    sess = None
            lastOp = op
        ## FOR
        
        return newSessions
    ## FOR

## CLASS