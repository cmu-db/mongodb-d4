#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest
import copy

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../.."))

# mongodb-d4
from tpcctestcase import TPCCTestCase as CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.network import NetworkCostComponent
from workload.workloadcombiner import WorkloadCombiner
from tpcc import constants as tpccConstants

class TestNetworkCostTPCC(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = NetworkCostComponent(self.state)
    ## DEF

    def testNetworkCostDenormalization(self):
        """Check network cost for queries that reference denormalized collections"""
        # Get the "base" design cost when all of the collections
        # are sharded on their "interesting" fields
        d = Design()
        i = 0
        for col_info in self.collections.itervalues():
            d.addCollection(col_info['name'])
            if i == 0:
                d.addShardKey(col_info['name'], col_info['interesting'])
            else:
                d.addShardKey(col_info['name'], ["_id"])
            
            self.cm.invalidateCache(d, col_info['name'])
            i += 1
        ## FOR
        self.cm.reset()
        self.state.reset()
        cost0 = self.cm.getCost(d)
        
        print "cost0:", cost0
        
        # Now get the network cost for when we denormalize the
        # second collection inside of the first one
        # We should have a lower cost because there should now be fewer queries
        d = Design()
        i = 0
        for col_info in self.collections.itervalues():
            self.assertTrue(col_info['interesting'])
            d.addCollection(col_info['name'])
            if i == 0:
                d.addShardKey(col_info['name'], col_info['interesting'])
            else:
                d.addShardKey(col_info['name'], ["_id"])
            self.cm.invalidateCache(d, col_info['name'])
            i += 1
            
        d.setDenormalizationParent(tpccConstants.TABLENAME_ORDER_LINE, tpccConstants.TABLENAME_ORDERS)
           
        combiner = WorkloadCombiner(self.collections, self.workload)
        combinedWorkload = combiner.process(d)
        self.state.updateWorkload(combinedWorkload)
        
        self.cm.reset()
        self.state.reset()
        cost1 = self.cm.getCost(d)
        print "cost1:", cost1
       
        self.assertLess(cost1, cost0)
   # DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN