#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
from costmodelcomponenttestcase import CostModelComponentTestCase
import costmodel

class TestCostModel(CostModelComponentTestCase):

    def setUp(self):
        CostModelComponentTestCase.setUp(self)
        self.cm = costmodel.CostModel(self.collections, self.workload, self.costModelConfig)
    ## DEF


    def testIgnore(self):
        pass

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN