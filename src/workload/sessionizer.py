# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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

import itertools
import logging
import math
import functools
from pprint import pformat

from util.histogram import Histogram

LOG = logging.getLogger(__name__)

class Sessionizer:
    """Takes a series of operations and figures out session boundaries"""
    
    def __init__(self):
        self.op_ctr = 0
        self.opHist = Histogram()
        self.prevOpHist = Histogram()
        self.nextOpHist = Histogram()
        self.sessionBoundaries = set()
        
        self.sessOps = { }
        pass
    ## DEF
    
    def process(self, sessId, operations):
        """Process a list of operations from a single connection."""
        self.sessOps[sessId] = [ ]
        lastOp = None
        ctr = 0
        for op in operations:
            if not "resp_time" in op: continue
            
            if lastOp:
                assert op["query_time"] >= lastOp["resp_time"]
                diff = op["query_time"] - lastOp["resp_time"]
                self.sessOps[sessId].append((lastOp, op, diff))
            lastOp = op
            ctr += 1
        ## FOR
        self.op_ctr += ctr
        LOG.debug("Examined %d operations for session %d [total=%d]" % (ctr, sessId, self.op_ctr))
    ## DEF
    
    def calculateSessions(self):
        # Calculate outliers using the quartile method
        # http://en.wikipedia.org/wiki/Quartile#Computing_methods
        LOG.info("Calculating time difference for operations in %d sessions" % len(self.sessOps))
        
        # Get the full list of all the time differences
        allDiffs = [ ]
        for clientOps in self.sessOps.values():
            allDiffs += [x[-1] for x in clientOps]
        allDiffs = sorted(allDiffs)
        numDiffs = len(allDiffs)

        # Calculate the median
        median = self.percentile(allDiffs, 0.50)
            
        stdDev = self.stddev(allDiffs)
        LOG.info("Operation Time Stddev: %.2f" % stdDev)
        
        # Now go through operations for each client and identify the
        # pairs of operations that are outliers
        for sessId, clientOps in self.sessOps.iteritems():
            for op0, op1, opDiff in clientOps:
                if opDiff > stdDev:
                    self.prevOpHist.put(op0["query_hash"])
                    self.nextOpHist.put(op1["query_hash"])
                    self.opHist.put((op0["query_hash"], op1["query_hash"]))
            ## FOR
        ## FOR
        
        print self.opHist
        sys.exit(1)
        
        # TODO: Now that we've populated these histograms, we need a way
        # to determine whether they are truly new sessions or not.

        # TODO: Once we have our session boundaries, we need to then
        # loop through each of the sessOps again and generate our
        # boundaries
        for sessId, clientOps in self.sessOps.iteritems():
            ip1, ip2, uid = sessId
            
            lastOp = None
            sess = None
            for op in clientOps:
                if lastOp:
                    sess["operations"].append(lastOp)
                    if (lastOp["query_hash"], op["query_hash"]) in self.sessionBoundaries:
                        sess = None
                if not sess:
                    sess = Session()
                    sess["ip_client"] = ip1
                    sess["ip_server"] = ip2
                    sess["session_id"] = uid
                    sess["operations"] = [ ]            
                lastOp = op
            ## FOR
            if lastOp: sess["operations"].append(lastOp)
        
        pass
    ## FOR
    
    # Copied from http://code.activestate.com/recipes/511478-finding-the-percentile-of-the-values/
    def percentile(N, percent, key=lambda x:x):
        """
        Find the percentile of a list of values.

        @parameter N - is a list of values. Note N MUST BE already sorted.
        @parameter percent - a float value from 0.0 to 1.0.
        @parameter key - optional key function to compute value from each element of N.

        @return - the percentile of the values
        """
        if not N:
            return None
        k = (len(N)-1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return key(N[int(k)])
        d0 = key(N[int(f)]) * (c-k)
        d1 = key(N[int(c)]) * (k-f)
        return d0+d1
    ## DEF
    # median is 50th percentile.
    median = functools.partial(percentile, percent=0.5)
    
    def stddev(self, x):
        """FROM: http://www.physics.rutgers.edu/~masud/computing/WPark_recipes_in_python.html"""
        n, mean, std = len(x), 0, 0
        for a in x:
            mean = mean + a
        mean /= float(n)
        for a in x:
            std = std + (a - mean)**2
        std = math.sqrt(std / float(n-1))
        return std
    
## CLASS