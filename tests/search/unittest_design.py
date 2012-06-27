#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import design 

class TestDesign (unittest.TestCase):
    
    def setUp(self) :
        pass

    @staticmethod
    def designFactory():
        design = design.Design()
        collections = ['col 1', 'col 2']
        design.addCollections(collections)
        design.addShardKey('col 1', ['c1b'])
        design.addShardKey('col 2', ['c2a'])
        design.addIndexes({ 'col 1' : [['c1a']], 'col 2' : [['c2c'], ['c2a', 'c2d']] })
        return design
        
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
        self.assertEqual(d.getShardKeys(collection), key)
        
    def testAddShardKeys(self) :
        d = design.Design()
        collections = ['test 1', 'test 2']
        d.addCollections(collections)
        keys = {}
        for col in collections :
            keys[col] = ['field 1']
        d.addShardKeys(keys)
        self.assertEqual(d.getAllShardKeys(), keys)
        
    def testAddIndex(self) :
        d = design.Design()
        collection = 'test 1'
        index = ['field 1', 'field 2']
        d.addCollection(collection)
        d.addIndex(collection, index)
        self.assertEqual(d.getIndexes(collection), [index])
        
    def testAddIndexes(self) :
        d = design.Design()
        collection = 'test 1'
        indexes = [['field 1'], ['field 2']]
        d.addCollection(collection)
        d.addIndexes({collection : indexes})
        self.assertEqual(d.getIndexes(collection), indexes)
    
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