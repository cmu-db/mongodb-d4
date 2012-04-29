#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import itertools
from search import *

class TestUtilMethods (unittest.TestCase):
    
    def setUp(self):
        pass
        
    def testBuildLoadingList(self) :
        # Denormalization Tree
        #    A
        #   / \
        #  B   C
        #  |
        #  D
        expected = [
            ['A'], ['B', 'C'], ['D']
        ]

        d = design.Design()
        d.addCollections(itertools.chain(*expected))
        d.setDenormalizationParent('B', 'A')
        d.setDenormalizationParent('C', 'A')
        d.setDenormalizationParent('D', 'B')
        print d
        
        loadOrder = utilmethods.buildLoadingList(d)
        print loadOrder
        self.assertNotEqual(loadOrder, None)
        
        # Go through each round and pop out collections
        # as we simulate them being loaded
        for loadRound in expected:
            while len(loadRound) > 0:
                collection = loadOrder.pop(0)
                self.assertNotEqual(collection, None)
                self.assertTrue(collection in loadRound)
                loadRound.remove(collection)
            ## WHILE
        ## FOR
        
        # Make sure that we processed all of our collections
        self.assertEqual(0, len(loadOrder))
        
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN