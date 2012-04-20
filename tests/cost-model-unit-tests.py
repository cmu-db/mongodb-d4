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
        self.d = search.Design.testFactory()
        self.w = workload.Workload.testFactory()
    
    def testNetworkCost(self) :
        query = workload.Query()
        query.collection = 'test'
        session = workload.Sess()
        session.queries.append(query)
        self.w.addSession(session)
        cost = self.cm.networkCost(self.d, self.w, 4)
        self.assertEqual(cost, 1.0)
        
    def testDiskCost(self) :
        cost = self.cm.diskCost(self.d, self.w)
        self.assertEqual(cost, 1.0)
    
    def testSkewCost(self) :
        cost = self.cm.skewCost(self.d, self.w)
        self.assertEqual(cost, 1.0)
    
    def testOverallCost(self) :
        config = {'nodes' : 4}
        cost = self.cm.overallCost(self.d, self.w, config)
        self.assertEqual(cost, 0.0)
    
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN