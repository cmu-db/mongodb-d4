#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import costmodel
import search
import workload

class TestCostModel (unittest.TestCase) :
    
    def setUp(self) :
        constants = {
           'alpha' : 0.0,
           'beta' : 0.0,
           'gamma' : 0.0
        }
        self.cm = costmodel.CostModel(constants)
        pass
        
    def testNetworkCost(self) :
        d = search.Design.testFactory()
        w = workload.Workload.testFactory()
        cost = self.cm.networkCost(d, w, 4)
        self.assertEqual(cost, 1.0)
        
    def testDiskCost(self) :
        self.assertEqual(True, True)
        
    def testSkewCost(self) :
        self.assertEqual(True, True)
        
    def testOverallCost(self) :
        self.assertEqual(True, True)
        
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN