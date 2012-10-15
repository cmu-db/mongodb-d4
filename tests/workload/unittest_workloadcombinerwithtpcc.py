#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../search"))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from workload.workloadcombiner import WorkloadCombiner
from tpcctestcase import TPCCTestCase as CostModelTestCase
from costmodel.disk import DiskCostComponent
from search import Design
from tpcc import constants as tpccConstants

class TestWorkloadCombiner(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
        ## DEF

    def testQueriesCombination(self):
        """Test if the total number of queries are reduced"""
        original_number_of_queries = 0
        for sess in self.workload:
            for op in sess["operations"]:
                original_number_of_queries += 1

        print "orignal number of queries: " + str(original_number_of_queries)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.collections, self.workload)

        # initialize a design with denormalization
        d = Design()
        for col_name in self.collections.iterkeys():
            d.addCollection(col_name)

        d.setDenormalizationParent(tpccConstants.TABLENAME_ORDER_LINE, tpccConstants.TABLENAME_ORDERS)

        combinedWorkload = combiner.process(d)

        number_of_queries_from_combined_workload = 0
        for sess in combinedWorkload:
            for op in sess["operations"]:
                number_of_queries_from_combined_workload += 1
                
        print "number of queries after query combination: " + str(number_of_queries_from_combined_workload)

        self.assertGreater(original_number_of_queries, number_of_queries_from_combined_workload)
        
    def testDiskCostNotChangedAfterQueryCombination(self):
        """Disk cost should not be changed after query combination"""
        d = Design()
        d = Design()
        for col_name in self.collections.iterkeys():
            d.addCollection(col_name)
        
        cost0 = self.cm.getCost(d)
        print "cost0 " + str(cost0)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.collections, self.workload)

        # initialize a design with denormalization
        d = Design()
        d = Design()
        for col_name in self.collections.iterkeys():
            d.addCollection(col_name)
            self.state.invalidateCache(col_name)
            
        d.setDenormalizationParent(tpccConstants.TABLENAME_ORDER_LINE, tpccConstants.TABLENAME_ORDERS)

        combinedWorkload = combiner.process(d)
        self.state.updateWorkload(combinedWorkload)
                
        self.cm.reset()
        self.cm.state.reset()
        cost1 = self.cm.getCost(d)

        print "cost1 " + str(cost1)
        
        self.assertEqual(cost0, cost1)
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN