#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
from costmodeltestcase import CostModelTestCase
import costmodel
from search import Design

class TestCostModel(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = costmodel.CostModel(self.collections, self.workload, self.costModelConfig)
    ## DEF

    def testSameDesignExecutedTwice_withemptydesign(self):
        """
            If the same design is executed twice, they should have the same result
        """
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)

        ## for 
        cost0 = self.cm.overallCost(d)
        cost1 = self.cm.overallCost(d)

        self.assertEqual(cost0, cost1)

    ## def
    
    def testSameDesignExecutedTwice_withfulldesign(self):
        """
            If the same design is executed twice, they should have the same result
        """
        d = Design()
        for col_name in CostModelTestCase.COLLECTION_NAMES:
            d.addCollection(col_name)
            col_info = self.collections[col_name]
            d.addIndex(col_name, col_info['interesting'])
        ## for
        
        cost0 = self.cm.overallCost(d)
        cost1 = self.cm.overallCost(d)

        self.assertEqual(cost0, cost1)
    ## def
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN