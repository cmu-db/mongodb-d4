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

import sys
import math
import random
import logging

# mongodb-d4
from util import *
from search import bbsearch
from abstractdesigner import AbstractDesigner

LOG = logging.getLogger(__name__)

# Constants
RELAX_RATIO_STEP = 0.1
RELAX_RATIO_UPPER_BOUND = 0.9
TIME_OUT_BBSEARCH = 9000

# Global Value
PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS = 0

## ==============================================
## LNSDesigner
## ==============================================
class LNSDesigner(AbstractDesigner):
    """
        Implementation of the large-neighborhood search design algorithm
    """

    def __init__(self, collections, designCandidates, workload, config, costModel, initialDesign, bestCost, timeout):
        AbstractDesigner.__init__(self, collections, workload, config)
        self.costModel = costModel
        self.initialDesign = initialDesign
        self.bestCost = bestCost
        self.timeout = timeout
        self.designCandidates = designCandidates
        self.roundCtr = 0
        self.relaxRatio = 0.25

        self.debug = False
        ### Test
        self.count = 0
    ## DEF

    def solve(self):
        """
            main public method. Simply call to get the optimal solution
        """
        bestDesign = self.initialDesign.copy()
        bestCost = self.bestCost
        table = TemperatureTable(self.collections)

        while True:
            relaxedCollectionsNames, relaxedDesign = self.__relax__(table, bestDesign)
            # when relax cannot make any progress
            if relaxedCollectionsNames is None and relaxedDesign is None:
                return bestDesign

            dc = self.designCandidates.getCandidates(relaxedCollectionsNames)
            bb = bbsearch.BBSearch(dc, self.costModel, relaxedDesign, bestCost, TIME_OUT_BBSEARCH)
            bbDesign = bb.solve()

            if bb.bestCost < bestCost:
                bestCost = bb.bestCost
                bestDesign = bbDesign
                
            if self.debug:
                LOG.info("\n======Relaxed Design=====\n%s", relaxedDesign)
                LOG.info("\n====Design Candidates====\n%s", dc)
                LOG.info("\n=====BBSearch Design=====\n%s", bbDesign)
                LOG.info("\n=====BBSearch Score======\n%s", bb.bestCost)
                LOG.info("\n========Best Score=======\n%s", bestCost)
                LOG.info("\n========Best Design======\n%s", bestDesign)

            self.relaxRatio += RELAX_RATIO_STEP
            if self.relaxRatio > RELAX_RATIO_UPPER_BOUND:
                self.relaxRatio = RELAX_RATIO_UPPER_BOUND
            self.timeout -= bb.usedTime

            if self.timeout <= 0:
                break
            self.roundCtr += 1

        self.bestCost = bestCost

        print "Best cost: ", bestCost
        return bestDesign
    # DEF

    def __relax__(self, table, design):
        numberOfRelaxedCollections = self.getNumberOfRelaxedCollections()

        # when numberOfRelaxedCollections reach the limit
        if numberOfRelaxedCollections is None:
            return None, None

        counter = 0
        collectionNameSet = set()
        relaxedCollectionsNames = []
        relaxedDesign = design.copy()

        while counter < numberOfRelaxedCollections:
            collectionName = table.getRandomCollection()
            if collectionName == "ORDER_LINE" and self.count <= 10:
                print "Happened!!!!!"
                self.count += 1
                continue
            if collectionName not in collectionNameSet:
                relaxedCollectionsNames.append(collectionName)
                collectionNameSet.add(collectionName)
                counter += 1

                if not relaxedDesign.hasCollection(collectionName):
                    relaxedDesign.addCollection(collectionName)
                relaxedDesign.reset(collectionName)

        return relaxedCollectionsNames, relaxedDesign

    def getNumberOfRelaxedCollections(self):
        global PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS

        num = int(round(len(self.collections.keys()) * self.relaxRatio))

        while num == PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS:
            self.relaxRatio += RELAX_RATIO_STEP

            # if the ratio is larger than one...the number of relaxed collection will
            # be larger than the number of collections...it doesn't sound good
            if self.relaxRatio > 1:
                return None
            num = int(round(len(self.collections.keys()) * self.relaxRatio))

        PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS = num

        return num
    ## DEF

class TemperatureTable():
    def __init__(self, collections):
        self.totalTemperature = 0.0
        self.temperatureList = []

        for coll in collections.itervalues():
            temperature = coll['data_size'] / coll['workload_queries']
            self.temperatureList.append((temperature, coll['name']))
            self.totalTemperature = self.totalTemperature + temperature

    def getRandomCollection(self):
        upper_bound = self.totalTemperature
        r = random.randint(0, int(self.totalTemperature))
        for temperature, name in sorted(self.temperatureList, reverse=True):
            lower_bound = upper_bound - temperature
            if lower_bound <= r <= upper_bound:
                return name
            upper_bound = lower_bound
