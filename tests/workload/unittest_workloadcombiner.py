#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from workload.workloadcombiner import WorkloadCombiner
from workloadcombinersetup import CostModelTestCase
from costmodel.disk import DiskCostComponent
from costmodel.network import NetworkCostComponent
from search import Design

class TestWorkloadCombiner(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = DiskCostComponent(self.state)
        self.cmn = NetworkCostComponent(self.state)
        self.col_names = [ x for x in self.collections.iterkeys()]
    ## DEF

    def testQueriesCombination(self):
        """Test if the total number of queries are reduced"""
        original_number_of_queries = 0
        for sess in self.workload:
            for op in sess["operations"]:
                original_number_of_queries += 1

        print "orignal number of queries: " + str(original_number_of_queries)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.col_names, self.workload)

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

    ## DEF
    
    def testDiskCostChangesAfterQueryCombination(self):
        """
            Assume we have collection A, B, C and we want to embed C to A
            If we build index on field00 of A and field02 of C
            The cost after query combination should be lower
        """
        d0 = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d0.addCollection(col_info['name'])
            d0.addIndex(col_info['name'], ['field00', 'field02'])
        
        cost0 = self.cm.getCost(d0)
        print "cost0 " + str(cost0)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.col_names, self.workload)

        # initialize a design with denormalization
        d1 = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d1.addCollection(col_info['name'])
            d1.addIndex(col_info['name'], ['field00', 'field02'])
            self.state.invalidateCache(col_info['name'])
            
        d1.setDenormalizationParent("koalas", "apples")

        combinedWorkload = combiner.process(d1)
        self.state.updateWorkload(combinedWorkload)
                
        self.cm.reset()
        self.cm.state.reset()
        cost1 = self.cm.getCost(d1)

        print "cost1 " + str(cost1)
        
        self.assertGreater(cost0, cost1)

        # Cost should remain the same after restoring the original workload
        self.state.restoreOriginalWorkload()
        self.cm.reset()
        self.cm.state.reset()
        cost2 = self.cm.getCost(d0)

        print "cost2 " + str(cost2)

        self.assertEqual(cost2, cost0)
    ## def
    
    def testNetworkCostShouldReduceAfterQueryCombination(self):
        """
            Network cost should be reduce after embedding collections
        """
        d0 = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d0.addCollection(col_info['name'])
            d0.addIndex(col_info['name'], ['field00', 'field02'])
        cost0 = self.cmn.getCost(d0)
        print "cost0 " + str(cost0)

        # Initialize a combiner
        combiner = WorkloadCombiner(self.col_names, self.workload)

        # initialize a design with denormalization
        d1 = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d1.addCollection(col_info['name'])
            d1.addIndex(col_info['name'], ['field00', 'field02'])
            self.state.invalidateCache(col_info['name'])

        d1.setDenormalizationParent("koalas", "apples")

        combinedWorkload = combiner.process(d1)
        self.state.updateWorkload(combinedWorkload)

        self.cmn.reset()
        self.cmn.state.reset()
        cost1 = self.cmn.getCost(d1)

        print "cost1 " + str(cost1)

        self.assertGreater(cost0, cost1)

        # Cost should remain the same after restoring the original workload
        self.state.restoreOriginalWorkload()
        self.cmn.reset()
        self.cmn.state.reset()
        cost2 = self.cmn.getCost(d0)

        print "cost2 " + str(cost2)

        self.assertEqual(cost2, cost0)
    ## def

    def testNotCollectionEmbeddingProcessShouldReturnNone(self):
        """
            If the given design has no collection embedding, we should return right away
        """
        d0 = Design()
        for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
            col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
            d0.addCollection(col_info['name'])
            d0.addIndex(col_info['name'], ['field00', 'field02'])

        # Initialize a combiner
        combiner = WorkloadCombiner(self.col_names, self.workload)

        combinedWorkload = combiner.process(d0)
        self.assertEqual(None, combinedWorkload)
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN