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
from math import sqrt
from pprint import pformat

from util.histogram import Histogram

LOG = logging.getLogger(__name__)

class Sessionizer:
    """Takes a stream of operations and figures out session boundaries"""
    
    def __init__(self):
        self.opHist = Histogram()
        self.prevOpHist = Histogram()
        self.nextOpHist = Histogram()
        self.sessionBoundaries = set()
        
        self.clientOperations = { }
        pass
    ## DEF
    
    def process(self, clientId, operations):
        """Process a list of operations from a single connection."""
        self.clientOperations[clientId] = [ ]
        lastOp = None
        for op in operations:
            if lastTimestamp:
                assert op["query_time"] >= lastOp["resp_time"]
                diff = op["query_time"] - lastOp["resp_time"]
                self.clientOperations.append((lastOp, op, diff))
            lastOp = op
        ## FOR
    ## DEF
    
    def calculateSessions(self):
        # First compute the standard deviation for the amount of
        # time between successive operations
        allDiffs = [ ]
        for clientOps in self.clientOperations.values():
            allDiffs += [x[-1] for x in clientOps]
        stdDev = self.stddev(allDiffs)
        
        # Now go through operations for each client and identify the
        # pairs of operations that are outliers
        for clientId, clientOps in self.clientOperations.iteritems():
            for op0, op1, opDiff in clientOps:
                if opDiff > stdDev:
                    self.prevOpHist.put(op0["query_hash"])
                    self.nextOpHist.put(op1["query_hash"])
                    self.opHist.put((op0["query_hash"], op1["query_hash"]))
            ## FOR
        ## FOR
        
        # TODO: Now that we've populated these histograms, we need a way
        # to determine whether they are truly new sessions or not.

        # TODO: Once we have our session boundaries, we need to then
        # loop through each of the clientOperations again and generate our
        # boundaries
        for clientId, clientOps in self.clientOperations.iteritems():
            ip1, ip2, uid = clientId
            
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
    
    def stddev(x):
        """FROM: http://www.physics.rutgers.edu/~masud/computing/WPark_recipes_in_python.html"""
        n, mean, std = len(x), 0, 0
        for a in x:
            mean = mean + a
        mean /= float(n)
        for a in x:
            std = std + (a - mean)**2
        std = sqrt(std / float(n-1))
        return std
    
## CLASS