#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

# mongodb-d4
from costmodeltestcase_index import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.disk import DiskCostComponent

class TestDiskCostIndexes(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
    # DEF
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

        # Now add the all indexes. The disk cost should be lower
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        d.addCollection(col_info['name'])
        d.addIndex(col_info['name'], col_info['interesting'])
        self.state.invalidateCache(col_info['name'])

        self.cm.reset()
        self.cm.state.reset()
        cost1 = self.cm.getCost(d)
        print "diskCost1:", cost1
        self.assertGreater(cost0, cost1)

    #def testDiskCostOnDifferentIndexes(self):
        #"""Check how indexes will affect the disk cost"""
        ## 1. Put index on both of the fields seperately
        #d = Design()
        #d.addCollection(CostModelTestCase.COLLECTION_NAME)
        #d.addIndex(CostModelTestCase.COLLECTION_NAME, ["field00"])
        #d.addIndex(CostModelTestCase.COLLECTION_NAME, ["field01"])

        #self.cm.reset()
        #self.cm.state.reset()
        #cost0 = self.cm.getCost(d)
        #print "diskCost0:", cost0

        ## 3. Put indexes on both field together
        #d = Design()
        #col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
        #d.addCollection(CostModelTestCase.COLLECTION_NAME)
        #d.addIndex(CostModelTestCase.COLLECTION_NAME, ["field01", "field00"])
        #self.state.invalidateCache(col_info['name'])

        #self.cm.reset()
        #self.cm.state.reset()
        #cost1 = self.cm.getCost(d)
        #print "diskCost1:", cost1

        #self.assertGreater(cost0, cost1)

    def testDiskCostCaching(self):
        """Check whether disk cost calculations work correctly with caching enabled"""
        self.cm.cache_enable = True

        # Give the mofo a full Design with indexes
        d = Design()
        col_info = self.collections[CostModelTestCase.COLLECTION_NAME]
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

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN
