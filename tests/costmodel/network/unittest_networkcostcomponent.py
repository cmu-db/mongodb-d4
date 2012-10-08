#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from costmodeltestcase import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.network import NetworkCostComponent
from workload.workloadcombiner import WorkloadCombiner

class TestNetworkCost(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = NetworkCostComponent(self.state)
    ## DEF

    def testNetworkCost(self):
        """Check network cost for equality predicate queries"""
        col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[0]]
        self.assertTrue(col_info['interesting'])

        # If we shard the collection on the interesting fields, then
        # each query should only need to touch one node
        d = Design()
        d.addCollection(col_info['name'])
        d.addShardKey(col_info['name'], col_info['interesting'])
        cost0 = self.cm.getCost(d)
        print "cost0: ", cost0

        # If we now shard the collection on just '_id', then every query
        # should have to touch every node. The cost of this design
        # should be greater than the first one
        d = Design()
        d.addCollection(col_info['name'])
        d.addShardKey(col_info['name'], ['_id'])
        self.cm.invalidateCache(d, col_info['name'])
        self.state.reset()
        cost1 = self.cm.getCost(d)
        print "cost1: ", cost1

        self.assertLess(cost0, cost1)
    ## DEF

    def testNetworkCostDenormalization(self):
       """Check network cost for queries that reference denormalized collections"""

       # Get the "base" design cost when all of the collections
       # are sharded on their "interesting" fields
       d = Design()
       for i in xrange(len(CostModelTestCase.COLLECTION_NAMES)):
           col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
           d.addCollection(col_info['name'])
           if i == 0:
               d.addShardKey(col_info['name'], col_info['interesting'])
           else:
               d.addShardKey(col_info['name'], ["_id"])
       ## FOR
       cost0 = self.cm.getCost(d)
       print "cost0:", cost0

       # Now get the network cost for when we denormalize the
       # second collection inside of the first one
       # We should have a lower cost because there should now be fewer queries
       
       d = Design()
       for i in xrange(0, len(CostModelTestCase.COLLECTION_NAMES)):
           col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
           self.assertTrue(col_info['interesting'])
           d.addCollection(col_info['name'])
           if i == 0:
               d.addShardKey(col_info['name'], col_info['interesting'])
           else:
               parent = self.collections[CostModelTestCase.COLLECTION_NAMES[0]]
               self.assertIsNotNone(parent)
               d.setDenormalizationParent(col_info['name'], parent['name'])
               print "parent: ", parent['name']
               print "child: ", col_info['name']
               self.assertTrue(d.isDenormalized(col_info['name']), col_info['name'])
               self.assertIsNotNone(d.getDenormalizationParent(col_info['name']))
           
           self.cm.invalidateCache(d, col_info['name'])
           
       combiner = WorkloadCombiner(self.collections, self.workload)
       combinedWorkload = combiner.process(d)
       self.state.updateWorkload(combinedWorkload)
       
       ## FOR
       self.cm.reset()
       self.state.reset()
       cost1 = self.cm.getCost(d)
       print "cost1:", cost1
       
       self.assertLess(cost1, cost0)
       
       # The denormalization cost should also be the same as the cost
       # when we remove all of the ops one the second collection
       for sess in self.state.workload:
           for op in sess["operations"]:
               if op["collection"] <> CostModelTestCase.COLLECTION_NAMES[0]:
                   print "This should not happen"
                   sess["operations"].remove(op)
           ## FOR (op)
       ## FOR (sess)
       for i in xrange(1, len(CostModelTestCase.COLLECTION_NAMES)):
           del self.collections[CostModelTestCase.COLLECTION_NAMES[i]]
       self.cm.reset()
       cost2 = self.cm.getCost(d)
       print "cost2:", cost2


       self.assertEqual(cost1, cost2)
   # DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN