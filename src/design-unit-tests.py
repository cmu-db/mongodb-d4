#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import search

class TestDesign (unittest.TestCase) :
    
    def setUp(self) :
        pass
        
    def testAddCollection(self) :
        design = search.Design()
        collection = 'Test'
        design.addCollection(collection)
        self.assertEqual(design.collections, [collection])
    
    def testAddCollections(self) :
        design = search.Design()
        collections = ['Test 1', 'Test 2']
        design.addCollections(collections)
        self.assertEqual(design.collections, collections)
                
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN