#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

from search import design

class TestDesign (unittest.TestCase):
    
    def setUp(self) :
        pass

    @staticmethod
    def designFactory():
        d = design.Design()
        collections = ['col 1', 'col 2']
        d.addCollections(collections)
        d.addShardKey('col 1', ['c1b'])
        d.addShardKey('col 2', ['c2a'])
        d.addIndex('col 1', ['c1a'])
        d.addIndex('col 2', ['c2c'])
        d.addIndex('col 2', ['c2a', 'c2d'])
        return d

    def testGetDelta(self):
        d0 = TestDesign.designFactory()
        d1 = TestDesign.designFactory()

        delta = d0.getDelta(d0)
        self.assertEqual(0, len(delta))

        delta = d0.getDelta(d1)
        self.assertEqual(0, len(delta))

        d2 = design.Design()
        for col_name in d0.getCollections():
            d2.addCollection(col_name)
            d2.addShardKey(col_name, d0.getShardKeys(col_name))
            for indexKeys in d0.getIndexes(col_name):
                d2.addIndex(col_name, indexKeys)
                break
        delta = d0.getDelta(d2)
        self.assertEqual(1, len(delta))

        # Throw an empty design at it. All of the collections
        # should come back in our delta
        d3 = design.Design()
        delta = d0.getDelta(d3)
        self.assertEquals(d0.getCollections(), delta)

    ## DEF

    def testAddCollection(self) :
        d = design.Design()
        collection = 'Test'
        d.addCollection(collection)
        self.assertEqual(d.getCollections(), [collection])
    
    def testAddCollections(self) :
        d = design.Design()
        collections = ['Test 1', 'Test 2']
        d.addCollections(collections)
        self.assertEqual(sorted(d.getCollections()), sorted(collections))
        
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
        index = ('field 1', 'field 2')
        d.addCollection(collection)
        d.addIndex(collection, index)
        self.assertListEqual(d.getIndexes(collection), [index])

    def testGetParentCollection(self):
        d = design.Design()
        d.addCollection('A')
        d.addCollection('B')
        d.setDenormalizationParent('B', 'A')
        self.assertFalse(d.isDenormalized('A'))
        self.assertTrue(d.isDenormalized('B'))
        self.assertEqual('A', d.getDenormalizationParent('B'))
    ## DEF

    def testGetDenormalizationHierarchy(self):
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

    def testGetCollectionsInTopologicalOrder(self):
        expected = {
            'A': ['B', 'C'],
            'B': ['D'],
            'C': [],
            'D': []
        }

        d = design.Design()
        d.addCollections(expected.keys())
        d.setDenormalizationParent('B', 'A')
        d.setDenormalizationParent('C', 'A')
        d.setDenormalizationParent('D', 'B')

        topologicalOrder = d.getCollectionsInTopologicalOrder()
        self.assertNotEqual(topologicalOrder, None)
        for collection in topologicalOrder.keys():
            topologicalOrder[collection] = sorted(topologicalOrder[collection])
        self.assertEqual(expected, topologicalOrder)

## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN