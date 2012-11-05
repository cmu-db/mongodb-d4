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
import os
import math
import random
import logging

# mongodb-d4
from util import *
from search import bbsearch
from abstractdesigner import AbstractDesigner

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../multithreaded"))

from message import *
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
    class RandomCollectionGenerator:
        def __init__(self, collections):
            self.rng = random.Random()
            self.collections = [ ]
            for col_name in collections.iterkeys():
                self.collections.append(col_name)
            ## FOR
            self.length = len(self.collections)
        ## DEF
        
        def getRandomCollection(self):
            r = self.rng.randint(0, self.length - 1)
            return self.collections[r]
        ## DEF
    ## CLASS
    
    def __init__(self, collections, designCandidates, workload, config, costModel, initialDesign, bestCost, timeout, channel=None, lock=None, outputfile=None):
        AbstractDesigner.__init__(self, collections, workload, config)
        self.costModel = costModel
        self.initialDesign = initialDesign
        self.bestCost = bestCost
        self.timeout = timeout
        self.designCandidates = designCandidates
        self.relaxRatio = 0.25

        self.channel = channel
        self.bbsearch_method = None
        self.bestLock = lock
        self.outputfile = outputfile
        
        self.debug = False
        ### Test
        self.count = 0
    ## DEF

    def run(self):
        """
            main public method. Simply call to get the optimal solution
        """
        LOG.info("Design candidates: \n%s", self.designCandidates)
        bestDesign = self.initialDesign.copy()
        col_generator = LNSDesigner.RandomCollectionGenerator(self.collections)
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
            
            relaxedCollectionsNames, relaxedDesign = self.__relax__(col_generator, bestDesign, self.relaxRatio)
            
            LOG.info("Relaxed collections\n %s", relaxedCollectionsNames)
            dc = self.designCandidates.getCandidates(relaxedCollectionsNames)
            self.bbsearch_method = bbsearch.BBSearch(dc, self.costModel, relaxedDesign, self.bestCost, bbsearch_time_out, self.channel, self.bestCost)
            bbDesign = self.bbsearch_method.solve()

            if self.bbsearch_method.bestCost < self.bestCost:
                LOG.info("LNSearch: Best score is updated from %s to %s", self.bestCost, self.bbsearch_method.bestCost)
                self.bestCost = self.bbsearch_method.bestCost
                bestDesign = bbDesign.copy()
                elapsedTime = 0
            else:
                elapsedTime += self.bbsearch_method.usedTime
            
            if isExhaustedSearch:
                elapsedTime = INIFITY
                
            if elapsedTime >= 2 * 60 * 60: # 1 hour
                # if it haven't found a better design for one hour, give up
                LOG.info("Haven't found a better design for %s minutes. QUIT", elapsedTime)
                break

            if self.debug:
                LOG.info("\n======Relaxed Design=====\n%s", relaxedDesign)
                LOG.info("\n====Design Candidates====\n%s", dc)
                LOG.info("\n=====BBSearch Design=====\n%s", bbDesign)
                LOG.info("\n=====BBSearch Score======\n%s", self.bbsearch_method.bestCost)
                LOG.info("\n========Best Score=======\n%s", self.bestCost)
                LOG.info("\n========Best Design======\n%s", bestDesign)

            self.relaxRatio += RELAX_RATIO_STEP
            if self.relaxRatio > RELAX_RATIO_UPPER_BOUND:
                self.relaxRatio = RELAX_RATIO_UPPER_BOUND
                
            self.timeout -= self.bbsearch_method.usedTime
            bbsearch_time_out += RELAX_RATIO_STEP / 0.1 * 30

            if self.timeout <= 0:
                break
        ## WHILE

        sendMessage(MSG_EXECUTE_COMPLETED, None, self.channel)
        LOG.info("Current thread is terminated")
        if self.outputfile:
            f = open(self.outputfile, 'w')
            f.write(finalSolution.toJSON())
            f.close()
        else:
            print finalSolution.toJSON()
    # DEF

    def __relax__(self, generator, design, ratio):
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
                collectionName = generator.getRandomCollection()
                if collectionName not in collectionNameSet:
                    relaxedCollectionsNames.append(collectionName)
                    collectionNameSet.add(collectionName)
                    relaxedDesign.reset(collectionName)
                    numberOfRelaxedCollections -= 1
                ## IF
            ## WHILE
        return relaxedCollectionsNames, relaxedDesign
    ## DEF
