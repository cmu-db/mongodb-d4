#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import random
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from costmodelcomponenttestcase import CostModelComponentTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.disk import DiskCostComponent

class TestDiskCostComponent(CostModelComponentTestCase):

    def setUp(self):
        CostModelComponentTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
    ## DEF

    def testDiskCost(self):
        """Check whether disk cost calculations work correctly"""

        # First get the disk cost when there are no indexes
        d = Design()
        for i in xrange(len(CostModelComponentTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelComponentTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        ## FOR
        cost0 = self.cm.getCost(d)
        print "diskCost0:", cost0
        # The cost should be exactly equal to one, which means that every operation
        # has to perform a full sequential scan on the collection
        self.assertEqual(cost0, 1.0)

        # Now add the indexes. The disk cost should be lower
        d = Design()
        for i in xrange(len(CostModelComponentTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelComponentTestCase.COLLECTION_NAMES[i]]
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
        for i in xrange(len(CostModelComponentTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelComponentTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            d.addIndex(col_info['name'], col_info['interesting'])
        ## FOR
        cost0 = self.cm.getCost(d)
        print "diskCost0:", cost0
        self.assertGreater(cost0, 0.0)

        # We should get the same cost back after we execute it a second time
        cost1 = self.cm.getCost(d)
        print "diskCost1:", cost1
        self.assertEqual(cost0, cost1)
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN