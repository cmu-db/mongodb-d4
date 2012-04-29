#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import design 

class TestDesign (unittest.TestCase) :
    
    def setUp(self) :
        pass
        
    def testAddCollection(self) :
        d = design.Design()
        collection = 'Test'
        d.addCollection(collection)
        self.assertEqual(d.getCollections(), [collection])
    
    def testAddCollections(self) :
        d = design.Design()
        collections = ['Test 1', 'Test 2']
        d.addCollections(collections)
        self.assertEqual(d.getCollections(), collections)
        
    def testAddShardKey(self) :
        d = design.Design()
        collection = 'test'
        key = ['field 1']
        d.addCollection(collection)
        d.addShardKey(collection, key)
        self.assertEqual(d.getShardKey(collection, key), key)
        
    def testAddShardKeys(self) :
        d = design.Design()
        collections = ['test 1', 'test 2']
        d.addCollections(collections)
        keys = {}
        for col in collections :
            keys[col] = ['field 1']
        d.addShardKeys(keys)
        self.assertEqual(d.getShardKeys(), keys)
        
    def testAddIndex(self) :
        d = design.Design()
        collection = 'test 1'
        index = ['field 1', 'field 2']
        d.addCollection(collection)
        d.addIndex(collection, index)
        self.assertEqual(d.getIndexesForCollection(collection), [index])
        
    def testAddIndexes(self) :
        d = design.Design()
        collection = 'test 1'
        indexes = [['field 1'], ['field 2']]
        d.addCollection(collection)
        d.addIndexes({collection : indexes})
        self.assertEqual(d.getIndexesforCollection(collection), indexes)
    
    def testDesignFactory(self) :
        d = design.Design.testFactory()
        self.assertEqual(isinstance(d, design.Design), True)
        
    def testGetDenormalizationHierarchy(self) :
        # Dependency Tree
        #    A
        #   / \
        #  B   C
        #  |
        #  D
        expected = {
            'A': [ ],
            'B': ['A'],
            'C': ['A'],
            'D': ['A', 'B']
        }

        d = design.Design()
        d.addCollections(expected.keys())
        d.setDenormalizationParent('B', 'A')
        d.setDenormalizationParent('C', 'A')
        d.setDenormalizationParent('D', 'B')
        print d
        
        for collection in d.getCollections():
            hierarchy = d.getDenormalizationHierarchy(collection)
            #print "-"*50
            #print collection, hierarchy
            self.assertNotEqual(hierarchy, None)
            self.assertTrue(collection in expected)
            self.assertEqual(expected[collection], hierarchy)
        ## FOR
    ## DEF
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN