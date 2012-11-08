#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../src"))
sys.path.append(os.path.join(basedir, "../../../src/search"))
sys.path.append(os.path.join(basedir, "../../../exps/benchmarks/replay"))

import unittest
from workloadgenerator import CostModelTestCase
from search import Design
from denormalizer import Denormalizer

class TestWorkloadCombiner(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.col_names = [ x for x in self.collections.iterkeys()]
    ## DEF
    
    def testDenormalizer(self):
        d = Design()
        for col_name in self.col_names:
            d.addCollection(col_name)
        ## FOR
        op_list = self.printOperations(self.workload)
        col_list = self.printAllCollections()
        d.setDenormalizationParent("koalas", "apples")
        
        dn = Denormalizer(self.metadata_db, self.dataset_db, d)
        new_workload = dn.process()
        
        new_op_list = self.printOperations(new_workload)
        new_col_list = self.printAllCollections()
        
        self.assertTrue("koalas" not in new_op_list)
        ## FOR
        
        self.assertTrue("koalas" not in new_col_list)
    ## DEF
    
    def printOperations(self, workload):
        op_list = []
        for sess in workload:
            for op in sess['operations']:
                op_list.append(op['collection'])
            ## FOR 
        ## FOR
        return op_list
    ## DEF
    
    def printAllCollections(self):
        col_list = [ ]
        for col_name in self.dataset_db.collection_names():
            col_list.append(col_name)
        ## FOR
        return col_list
    ## DEF
    
if __name__ == '__main__':
    unittest.main()
## MAIN