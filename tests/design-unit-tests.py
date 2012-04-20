#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import design 

class TestDesign (unittest.TestCase) :
    
    def setUp(self) :
        pass
        
    def testAddCollection(self) :
        design = design.Design()
        collection = 'Test'
        design.addCollection(collection)
        self.assertEqual(design.collections, [collection])
    
    def testAddCollections(self) :
        design = design.Design()
        collections = ['Test 1', 'Test 2']
        design.addCollections(collections)
        self.assertEqual(design.collections, collections)
                
    def testAddFieldsOneCollection(self) :
        design = design.Design()
        collection = 'test'
        design.addCollection(collection)
        fields = ['field 1', 'field 2']
        design.addFieldsOneCollection(collection, fields)
        self.assertEqual(design.fields, {collection : fields})
        
    def testAddFields(self) :
        design = design.Design()
        collections = ['test 1', 'test 2']
        fields = {}
        for col in collections :
            fields[col] = ['test 1', 'test 2']
        design.addCollections(collections)
        design.addFields(fields)
        self.assertEqual(design.fields, fields)
        
    def testAddShardKey(self) :
        design = design.Design()
        collection = 'test'
        key = 'field 1'
        design.addShardKey(collection, key)
        self.assertEqual(design.shardKeys, {collection : key})
        
    def testAddShardKeys(self) :
        design = design.Design()
        collections = ['test 1', 'test 2']
        keys = {}
        for col in collections :
            keys[col] = 'field 1'
        design.addShardKeys(keys)
        self.assertEqual(design.shardKeys, keys)
        
    def testAddIndex(self) :
        design = design.Design()
        collection = 'test 1'
        index = ['field 1', 'field 2']
        design.addCollection(collection)
        design.addIndex(collection, index)
        self.assertEqual(design.indexes, {collection : [index]})
        
    def testAddIndexes(self) :
        design = design.Design()
        collection = 'test 1'
        indexes = [['field 1'], ['field 2']]
        design.addCollection(collection)
        design.addIndexes({collection : indexes})
        self.assertEqual(design.indexes, {collection : indexes})
    
    def testDesignFactory(self) :
        design = design.Design.testFactory()
        self.assertEqual(isinstance(design, design.Design), True)
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN