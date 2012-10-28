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
RELAX_RATIO_UPPER_BOUND = 0.5
INIFITY = float('inf')

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
        self.relaxRatio = 0.25

        self.debug = False
        ### Test
        self.count = 0
    ## DEF

    def solve(self):
        """
            main public method. Simply call to get the optimal solution
        """
        LOG.info("Design candidates: \n%s", self.designCandidates)
        bestDesign = self.initialDesign.copy()
        table = TemperatureTable(self.collections)
        elapsedTime = 0
        isExhaustedSearch = False
        # If we have 4 or less collections, we run bbsearch till it finishes
        if len(self.collections) <= constants.EXAUSTED_SEARCH_BAR:
            LOG.info("Infinity Mode is ON!!!")
            bbsearch_time_out = INIFITY # as long as possible
            isExhaustedSearch = True
            self.relaxRatio = 1.0
        else:
            bbsearch_time_out = 10 * 60 # 10 minutes
        while True:
            LOG.info("Started one bbsearch, current bbsearch_time_out is: %s, relax ratio is: %s", bbsearch_time_out, self.relaxRatio)
            relaxedCollectionsNames, relaxedDesign = self.__relax__(table, bestDesign, self.relaxRatio)
            dc = self.designCandidates.getCandidates(relaxedCollectionsNames)
            bb = bbsearch.BBSearch(dc, self.costModel, relaxedDesign, self.bestCost, bbsearch_time_out)
            bbDesign = bb.solve()

            if bb.bestCost < self.bestCost:
                LOG.info("LNSearch: Best score is updated from %s to %s", self.bestCost, bb.bestCost)
                self.bestCost = bb.bestCost
                bestDesign = bbDesign.copy()
                elapsedTime = 0
            else:
                elapsedTime += bb.usedTime
            
            if isExhaustedSearch:
                elapsedTime = INIFITY
                
            if elapsedTime >= 60 * 60: # 1 hour
                # if it haven't found a better design for one hour, give up
                LOG.info("Haven't found a better design for %s minutes. QUIT", elapsedTime)
                break

            if self.debug:
                LOG.info("\n======Relaxed Design=====\n%s", relaxedDesign)
                LOG.info("\n====Design Candidates====\n%s", dc)
                LOG.info("\n=====BBSearch Design=====\n%s", bbDesign)
                LOG.info("\n=====BBSearch Score======\n%s", bb.bestCost)
                LOG.info("\n========Best Score=======\n%s", self.bestCost)
                LOG.info("\n========Best Design======\n%s", bestDesign)

            self.relaxRatio += RELAX_RATIO_STEP
            if self.relaxRatio > RELAX_RATIO_UPPER_BOUND:
                self.relaxRatio = RELAX_RATIO_UPPER_BOUND
                
            self.timeout -= bb.usedTime
            bbsearch_time_out += RELAX_RATIO_STEP / 0.1 * 30

            if self.timeout <= 0:
                break
        ## WHILE

        LOG.info("Final Cost: %s", self.bestCost)
        return bestDesign
    # DEF

    def __relax__(self, table, design, ratio):
        numberOfRelaxedCollections = int(round(len(self.collections) * ratio))
        relaxedDesign = design.copy()
        
        if numberOfRelaxedCollections == len(self.collections):
            for col_name in self.collections:
                relaxedDesign.reset(col_name)
            relaxedCollectionsNames = self.collections.keys()[:]
            ## FOR
        ## IF
        else:
            collectionNameSet = set()
            relaxedCollectionsNames = []
            while numberOfRelaxedCollections > 0:
                collectionName = table.getRandomCollection()
                if collectionName not in collectionNameSet:
                    relaxedCollectionsNames.append(collectionName)
                    collectionNameSet.add(collectionName)
                    relaxedDesign.reset(collectionName)
                    numberOfRelaxedCollections -= 1
                ## IF
            ## WHILE
        return relaxedCollectionsNames, relaxedDesign
    ## DEF
    
class TemperatureTable():
    def __init__(self, collections):
        self.totalTemperature = 0.0
        self.temperatureList = []
        self.rng = random.Random()
        for coll in collections.itervalues():
            temperature = coll['data_size'] / coll['workload_queries']
            self.temperatureList.append((temperature, coll['name']))
            self.totalTemperature = self.totalTemperature + temperature

    def getRandomCollection(self):
        upper_bound = self.totalTemperature
        r = self.rng.randint(0, int(self.totalTemperature))
        for temperature, name in sorted(self.temperatureList, reverse=True):
            lower_bound = upper_bound - temperature
            if lower_bound <= r <= upper_bound:
                return name
            upper_bound = lower_bound
