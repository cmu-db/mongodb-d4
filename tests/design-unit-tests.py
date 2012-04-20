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
        self.assertEqual(d.collections, [collection])
    
    def testAddCollections(self) :
        d = design.Design()
        collections = ['Test 1', 'Test 2']
        d.addCollections(collections)
        self.assertEqual(d.collections, collections)
                
    def testAddFieldsOneCollection(self) :
        d = design.Design()
        collection = 'test'
        d.addCollection(collection)
        fields = ['field 1', 'field 2']
        d.addFieldsOneCollection(collection, fields)
        self.assertEqual(d.fields, {collection : fields})
        
    def testAddFields(self) :
        d = design.Design()
        collections = ['test 1', 'test 2']
        fields = {}
        for col in collections :
            fields[col] = ['test 1', 'test 2']
        d.addCollections(collections)
        d.addFields(fields)
        self.assertEqual(d.fields, fields)
        
    def testAddShardKey(self) :
        d = design.Design()
        collection = 'test'
        key = 'field 1'
        d.addShardKey(collection, key)
        self.assertEqual(d.shardKeys, {collection : key})
        
    def testAddShardKeys(self) :
        d = design.Design()
        collections = ['test 1', 'test 2']
        keys = {}
        for col in collections :
            keys[col] = 'field 1'
        d.addShardKeys(keys)
        self.assertEqual(d.shardKeys, keys)
        
    def testAddIndex(self) :
        d = design.Design()
        collection = 'test 1'
        index = ['field 1', 'field 2']
        d.addCollection(collection)
        d.addIndex(collection, index)
        self.assertEqual(d.indexes, {collection : [index]})
        
    def testAddIndexes(self) :
        d = design.Design()
        collection = 'test 1'
        indexes = [['field 1'], ['field 2']]
        d.addCollection(collection)
        d.addIndexes({collection : indexes})
        self.assertEqual(d.indexes, {collection : indexes})
    
    def testDesignFactory(self) :
        d = design.Design.testFactory()
        self.assertEqual(isinstance(d, design.Design), True)
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN