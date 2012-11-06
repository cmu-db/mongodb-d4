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
        
        def getRandomCollections(self, num):
            r = self.rng.sample(self.collections, num)
            return r
        ## DEF
    ## CLASS
    
    def __init__(self, collections, designCandidates, workload, config, costModel, initialDesign, bestCost, channel=None, lock=None, outputfile=None):
        AbstractDesigner.__init__(self, collections, workload, config)
        self.costModel = costModel
        
        self.init_bestDesign = initialDesign.copy()
        self.init_bestCost = bestCost
        
        self.timeout = self.config.getint(configutil.SECT_MULTI_SEARCH, 'time_for_lnssearch')
        self.patient_time = self.config.getint(configutil.SECT_MULTI_SEARCH, 'patient_time')
        
        # If we have 4 or less collections, we run bbsearch till it finishes
        if len(self.collections) <= constants.EXAUSTED_SEARCH_BAR:
            self.init_bbsearch_time = INIFITY
            self.init_relaxRatio = 1.0
            self.isExhaustedSearch = True
        else:
            self.init_bbsearch_time = self.config.getint(configutil.SECT_MULTI_SEARCH, 'init_bbsearch_time')
            self.init_relaxRatio = self.config.getfloat(configutil.SECT_MULTI_SEARCH, 'init_relax_ratio')
            self.isExhaustedSearch = False
            
        self.ratio_step = self.config.getfloat(configutil.SECT_MULTI_SEARCH, 'relax_ratio_step')
        self.max_ratio = self.config.getfloat(configutil.SECT_MULTI_SEARCH, 'max_relax_ratio')
        
        self.designCandidates = designCandidates

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
        col_generator = LNSDesigner.RandomCollectionGenerator(self.collections)
        
        elapsedTime = 0
        relaxRatio = self.init_relaxRatio
        bbsearch_time_out = self.init_bbsearch_time
        bestCost = self.init_bestCost
        bestDesign = self.init_bestDesign.copy()
        
        while True:
            relaxedCollectionsNames, relaxedDesign = self.__relax__(col_generator, bestDesign, relaxRatio)
            sendMessage(MSG_SEARCH_INFO, (relaxedCollectionsNames, bbsearch_time_out, relaxedDesign, bestDesign), self.channel)
            
            dc = self.designCandidates.getCandidates(relaxedCollectionsNames)
            self.bbsearch_method = bbsearch.BBSearch(dc, self.costModel, relaxedDesign, bestCost, bbsearch_time_out, self.channel, self.bestLock)
            bbDesign, bbCost = self.bbsearch_method.solve()

            if self.bbsearch_method.status == "solved":
                if bbCost < bestCost:
                    bestCost = bbCost
                    bestDesign = bbDesign.copy()
                    elapsedTime = 0
                else:
                    elapsedTime += self.bbsearch_method.usedTime
                
                if self.isExhaustedSearch:
                    elapsedTime = INIFITY
                    
                if elapsedTime >= self.patient_time:
                    # if it haven't found a better design for one hour, give up
                    LOG.info("Haven't found a better design for %s minutes. QUIT", elapsedTime)
                    break

                if self.debug:
                    LOG.info("\n======Relaxed Design=====\n%s", relaxedDesign)
                    LOG.info("\n====Design Candidates====\n%s", dc)
                    LOG.info("\n=====BBSearch Design=====\n%s", bbDesign)
                    LOG.info("\n=====BBSearch Score======\n%s", bbCost)
                    LOG.info("\n========Best Score=======\n%s", bestCost)
                    LOG.info("\n========Best Design======\n%s", bestDesign)

                relaxRatio += self.ratio_step
                if relaxRatio > self.max_ratio:
                    relaxRatio = self.max_ratio
                    
                self.timeout -= self.bbsearch_method.usedTime
                bbsearch_time_out += self.ratio_step / 0.1 * 30

                if self.timeout <= 0:
                    break
            ## IF
            else:
                # If we terminate bbsearch on purpose, we use the current best design of bbsearch as the bestDesign for next round
                # Also, since we have the current best design, we want to reset the relax ratio to the initial ratio because the
                # current design is good
                # OH, also the bbsearch_time
                relaxRatio = self.init_relaxRatio
                bbsearch_time_out = self.init_bbsearch_time
                bestCost = bbCost
                bestDesign = bbDesign.copy()
            ## ELSE
        ## WHILE

        sendMessage(MSG_EXECUTE_COMPLETED, None, self.channel)
        LOG.info("Current thread is terminated")
        if self.outputfile:
            f = open(self.outputfile, 'w')
            f.write(bestDesign.toJSON())
            f.close()
        else:
            print bestDesign.toJSON()
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
            relaxedCollectionsNames = generator.getRandomCollections(numberOfRelaxedCollections)
            for col_name in relaxedCollectionsNames:
                relaxedDesign.reset(col_name)
            ## FOR
            
        return relaxedCollectionsNames, relaxedDesign
    ## DEF
