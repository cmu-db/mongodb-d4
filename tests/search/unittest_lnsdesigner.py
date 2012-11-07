#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

from search.lnsdesigner import LNSDesigner

class TestSearchSpace (unittest.TestCase) :

    def setUp(self):
        self.collections = { }
        for i in xrange(100):
            self.collections["key" + str(i)] = i
        pass
    ## DEF

    def testRandomCollectionGenerator(self):
        """
            Check whether RandomCollectionGenerator can generate random collections
        """
        rcg = LNSDesigner.RandomCollectionGenerator(self.collections)
        map_round_to_set = { }
        for j in xrange(3):
            map_round_to_set[j] = rcg.getRandomCollections(3)
        ## FOR
        
        value_list = [val for val in map_round_to_set.itervalues()] 
        
        self.assertNotEqual(sorted(value_list[0]), sorted(value_list[1]))
        self.assertNotEqual(sorted(value_list[0]), sorted(value_list[2]))
        self.assertNotEqual(sorted(value_list[1]), sorted(value_list[2]))
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN