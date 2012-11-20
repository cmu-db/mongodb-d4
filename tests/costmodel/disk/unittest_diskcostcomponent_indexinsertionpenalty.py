#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

# mongodb-d4
from costmodeltestcase import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.disk import DiskCostComponent

class TestDiskCost_IndexInsertionPenalty(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
    # DEF

    def testDiskCost_IndexInsertionPenalty(self):
        """
            IndexInsertionPenalty should be high if we build bad indexes
        """
        # 1
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field00"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p0 = self.cm.total_index_insertion_penalty
        
        # 2
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field01"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p1 = self.cm.total_index_insertion_penalty
        
        self.assertEqual(p0, p1)
        
        #3
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field00", "field01"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p2 = self.cm.total_index_insertion_penalty
        
        self.assertEqual(p0, p2)
        
        #4
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field00", "field02"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p3 = self.cm.total_index_insertion_penalty
        
        self.assertGreater(p3, p0)
        
        #5
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field01", "field02"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p4 = self.cm.total_index_insertion_penalty
        
        self.assertGreater(p4, p0)
        
        #6
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            d.addIndex(col_name, ["field00", "field01", "field02"])
        ## FOR

        self.cm.reset()
        self.cm.state.reset()
        self.cm.getCost(d)
        p5 = self.cm.total_index_insertion_penalty
        
        self.assertGreater(p5, p0)
    ## DEF
    
    def testDiskCost_IndexInsertionPenalty_integrated_to_cost_component(self):
        """
            Check if index insertion penalty contributes to the total diskcost
        """
        
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN
