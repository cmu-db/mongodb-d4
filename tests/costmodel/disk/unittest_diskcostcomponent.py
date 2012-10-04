#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from costmodeltestcase import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.disk import DiskCostComponent

class TestDiskCost(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
    ## DEF

    def testDiskCostIndexes(self):
        """Check whether disk cost calculations work correctly"""

        # First get the disk cost when there are no indexes
        d = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        ## FOR
        cost0 = self.cm.getCost(d)
        print "diskCost0:", cost0
        # The cost should be exactly equal to one, which means that every operation
        # has to perform a full sequential scan on the collection
        self.assertEqual(cost0, 1.0)

        # Now add the indexes. The disk cost should be lower
        d = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            d.addIndex(col_info['name'], col_info['interesting'])
            self.state.invalidateCache(col_info['name'])
        ## FOR
        cost1 = self.cm.getCost(d)
        print "diskCost1:", cost1
        self.assertGreater(cost0, cost1)
    ## DEF

    def testDiskCostCaching(self):
        """Check whether disk cost calculations work correctly with caching enabled"""
        self.cm.cache_enable = True

        # Give the mofo a full Design with indexes
        d = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            d.addIndex(col_info['name'], col_info['interesting'])
        ## FOR
        cost0 = self.cm.getCost(d)
        print "diskCost0:", cost0
        # FIXME self.assertGreater(cost0, 0.0)

        # We should get the same cost back after we execute it a second time
        cost1 = self.cm.getCost(d)
        print "diskCost1:", cost1
        # FIXME self.assertEqual(cost0, cost1)
    ## DEF

    def testEstimateWorkingSets(self):
        """Check the working set size estimator for collections"""

        d = Design()
        for i in xrange(0, len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        ## FOR

        max_memory = self.costModelConfig['max_memory'] * 1024 * 1024
        workingSets = self.cm.estimateWorkingSets(d, max_memory)
        self.assertIsNotNone(workingSets)

        for i in xrange(0, len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            self.assertIn(col_info['name'], workingSets)
            setSize = workingSets[col_info['name']]
            print col_info['name'], "->", setSize
            self.assertGreater(setSize, 0.0)
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN