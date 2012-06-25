#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import string
import random
import unittest
from pprint import pprint, pformat

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../src"))
from inputs.mongodb.reconstructor import Reconstructor

class TestReconstructor(unittest.TestCase):

    def setUp(self):
        pass

    def testReconstructDatabase(self):
        pass

    def testExtractSchema(self):
        pass

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN