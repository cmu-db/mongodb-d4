#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

# mongodb-d4
from costmodeltestcase_index_withprojection import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.disk import DiskCostComponent

class TestDiskCostIndexesWithProjection(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
        
    ## DEF
    def testDiskCostIndexes(self):
        """Check whether disk cost calculations work correctly"""
        # First get the disk cost when there are no indexes
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        d.addCollection(col_info['name'])

        cost0 = self.cm.getCost(d)
        print "diskCost0:", cost0
        # The cost should be exactly equal to one, which means that every operation
        # has to perform a full sequential scan on the collection
        self.assertEqual(cost0, 1.0)

        # Now add one index. The disk cost should be lower
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        d.addCollection(col_info['name'])
        d.addIndex(col_info['name'], ["field01"])
        self.state.invalidateCache(col_info['name'])

        self.cm.reset()
        self.cm.state.reset()
        cost1 = self.cm.getCost(d)
        print "diskCost1:", cost1
        self.assertGreater(cost0, cost1)
        
        # Now add one more index. The disk cost should be lower again
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        d.addCollection(col_info['name'])
        d.addIndex(col_info['name'], ["field01", "field00"])
        self.state.invalidateCache(col_info['name'])

        self.cm.reset()
        self.cm.state.reset()
        cost2 = self.cm.getCost(d)
        print "diskCost2:", cost2
        
        # Now add the one index. The disk cost should be much lower
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        d.addCollection(col_info['name'])
        d.addIndex(col_info['name'], ["field01", "field00", "field02"])
        self.state.invalidateCache(col_info['name'])

        self.cm.reset()
        self.cm.state.reset()
        cost3 = self.cm.getCost(d)
        print "diskCost3:", cost3
        self.assertGreater(cost2, cost3)

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN
