#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from pprint import pprint, pformat

from util import mathutil

class TestMathUtil(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def testPercentile(self):
        data = [
            (range(10), 0.25, 2.25),
            (range(10), 0.75, 6.75),
            (range(10), 0.50, 4.5),
            (range(11), 0.50, 5)
        ]
        for values, p, expected in data:
            actual = mathutil.percentile(values, p)
            self.assertEqual(expected, actual)
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN