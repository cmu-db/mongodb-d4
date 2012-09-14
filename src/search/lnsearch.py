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


## ==============================================
## Large Neighborhood Search
## ==============================================
import random
from util import *
from search import DesignCandidates, bbsearch
import logging
import math
import sys

LOG = logging.getLogger(__name__)

# Constants
RELAX_RATIO_STEP = 0.1
RELAX_RATIO_UPPER_BOUND = 0.5
TIME_OUT_BBSEARCH = 2

# Global Value
PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS = 0

class LNSearch():
    """
        Implementation of the large-neighborhood search design algorithm
    """

    def __init__(self, cparser, collectionDict, costModel, initialDesign, bestCost, timeout):
        self.collectionDict = collectionDict
        self.costModel = costModel
        self.initialDesign = initialDesign
        self.bestCost = bestCost
        self.timeout = timeout
        self.cparser = cparser

        self.relaxRatio = 0.25
        
        self.debug = LOG.isEnabledFor(logging.debug)
    ## DEF

    def solve(self):
        """
            main public method. Simply call to get the optimal solution
        """
        BestDesign = self.initialDesign.copy()
        BestCost = self.bestCost
        table = TemperatureTable(self.collectionDict)
        
        while self.relaxRatio <= RELAX_RATIO_UPPER_BOUND:
            relaxedCollections, relaxedDesign = self.relax(table, BestDesign)

            # when relax cannot make any progress
            if relaxedCollections is None and relaxedDesign is None:
                return BestDesign

            dc = self.generateDesignCandidates(relaxedCollections)
            bb = bbsearch.BBSearch(dc, self.costModel, relaxedDesign, BestCost, TIME_OUT_BBSEARCH)
            bbDesign = bb.solve()
            
            if self.debug:
                LOG.info("\n======Relaxed Design=====\n%s", relaxedDesign.data)
                LOG.info("\n====Design Candidates====\n%s", dc)
                LOG.info("\n=====BBSearch Design=====\n%s", bbDesign.data)
                LOG.info("\n=====BBSearch Score======\n%s", bb.bestCost)
                LOG.info("\n========Best Score=======\n%f", BestCost)
                
            if bb.bestCost < BestCost:
                BestCost = bb.bestCost
                BestDesign = bbDesign
            
            self.relaxRatio += RELAX_RATIO_STEP
            self.timeout -= bb.usedTime

            if self.timeout <= 0:
                break

        self.bestCost = BestCost

        return BestDesign
    # DEF
    
    def relax(self, table, design):
        numberOfRelaxedCollections = self.getNumberOfRelaxedCollections()

        # when numberOfRelaxedCollections reach the limit
        if numberOfRelaxedCollections is None:
            return None, None
        
        counter = 0
        collectionNameSet = set()
        relaxedCollections = []
        relaxedDesign = design.copy()
        
        while counter < numberOfRelaxedCollections:
            collection = table.getRandomCollection();
            collectionName = collection['name']

            if collectionName not in collectionNameSet:
                collectionNameSet.add(collectionName)
                counter += 1
                
                if not relaxedDesign.hasCollection(collection['name']):
                    relaxedDesign.addCollection(collection['name'])
                relaxedDesign.reset(collection['name'])
                relaxedCollections.append(collection)
        
        return relaxedCollections, relaxedDesign

    def getNumberOfRelaxedCollections(self):
        global PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS

        num = int(round(len(self.collectionDict.keys()) * self.relaxRatio))

        while num == PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS:
            self.relaxRatio += RELAX_RATIO_STEP

            # if the ratio is larger than one...the number of relaxed collection will
            # be larger than the number of collections...it doesn't sound good
            if self.relaxRatio > 1:
                return None
            num = int(round(len(self.collectionDict.keys()) * self.relaxRatio))

        PREVIOUS_NUMBER_OF_RELAXED_COLLECTIONS = num

        return num

    def generateDesignCandidates(self, collections):

        isShardingEnabled = self.cparser.get(SECT_DESIGNER, 'enable_sharding')
        isIndexesEnabled = self.cparser.get(SECT_DESIGNER, 'enable_indexes')
        isDenormalizationEnabled = self.cparser.get(SECT_DESIGNER, 'enable_denormalization')
        
        shardKeys = []
        indexKeys = [[]]
        denorm = []
        dc = DesignCandidates()
        
        for col_info in collections:
            interesting = col_info['interesting']
            if constants.SKIP_MONGODB_ID_FIELD and "_id" in interesting:
                interesting = interesting[:]
                interesting.remove("_id")

            # deal with shards
            if isShardingEnabled == 'True':
                if self.debug: LOG.debug("Sharding is enabled")
                shardKeys = interesting

            # deal with indexes
            if isIndexesEnabled == 'True':
                if self.debug: LOG.debug("Indexes is enabled")
                for o in xrange(1, len(interesting) + 1) :
                    for i in itertools.combinations(interesting, o) :
                        indexKeys.append(i)

            # deal with de-normalization
            if isDenormalizationEnabled == 'True':
                if self.debug: LOG.debug("Demormalization is enabled")
                for k,v in col_info['fields'].iteritems() :
                    if v['parent_col'] <> '' and v['parent_col'] not in denorm :
                        denorm.append(v['parent_col'])
                        
            dc.addCollection(col_info['name'], indexKeys, shardKeys, denorm)
        ## FOR
        return dc
    ## DEF

class TemperatureTable():
    def __init__(self, collectionDict):
        self.totalTemperature = 0.0
        self.temperatureList = []
        
        for coll in collectionDict.itervalues():
            temperature = coll['data_size'] / coll['workload_queries']
            self.temperatureList.append((temperature, coll)) 
            self.totalTemperature = self.totalTemperature + temperature
    
    def getRandomCollection(self):
        r = random.randint(0, int(self.totalTemperature))
        for temperature, coll in reversed(sorted(self.temperatureList)):
            r_ratio = r / self.totalTemperature
            cur_ratio = temperature / self.totalTemperature
            if r_ratio >= cur_ratio:
                return coll
        
        return sorted(self.temperatureList)[0][1]
