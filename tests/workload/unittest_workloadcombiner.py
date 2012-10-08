#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from workload.workloadcombiner import WorkloadCombiner
from workloadcombinersetup import CostModelTestCase
from costmodel.disk import DiskCostComponent
from search import Design

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
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])

        d.setDenormalizationParent("koalas", "apples")

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
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        
        cost0 = self.cm.getCost(d)
        print "cost0 " + str(cost0)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.collections, self.workload)

        # initialize a design with denormalization
        d = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            self.state.invalidateCache(col_info['name'])
            
        d.setDenormalizationParent("koalas", "apples")

        combinedWorkload = combiner.process(d)
        self.state.workload = combinedWorkload
        
        cost1 = self.cm.getCost(d)

        print "cost1 " + str(cost1)
        
        self.assertEqual(cost0, cost1)
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN