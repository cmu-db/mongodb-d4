#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from pprint import pprint, pformat

from util import config

class TestConfig(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def testMakeDefaultConfig(self):
        c = config.makeDefaultConfig()
        self.assertIsNotNone(c)
        for sect in config.ALL_SECTIONS:
            self.assertIn(sect, c.sections())
            for key, desc, default in config.DEFAULT_CONFIG[sect]:
                self.assertIn(key, c.options(sect))
                self.assertEqual(default, c.get(sect, key))
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN