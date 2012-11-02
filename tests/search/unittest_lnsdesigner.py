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
        for i in xrange(10):
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
            tmp = set()
            count = 0
            while count < 3:
                col_name = rcg.getRandomCollection()
                if col_name not in tmp:
                    tmp.add(col_name)
                    count += 1
            ## FOR
            map_round_to_set[j] = tmp
        ## FOR
        
        value_list = [val for val in map_round_to_set.itervalues()] 
        
        self.assertNotEqual(value_list[0], value_list[1])
        self.assertNotEqual(value_list[0], value_list[2])
        self.assertNotEqual(value_list[1], value_list[2])
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN