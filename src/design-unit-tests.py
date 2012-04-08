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
                
    def testAddFieldsOneCollection(self) :
        design = search.Design()
        collection = 'test'
        design.addCollection(collection)
        fields = ['field 1', 'field 2']
        design.addFieldsOneCollection(collection, fields)
        self.assertEqual(design.fields, {collection : fields})
        
    def testAddFields(self) :
        design = search.Design()
        collections = ['test 1', 'test 2']
        fields = {}
        for col in collections :
            fields[col] = ['test 1', 'test 2']
        design.addCollections(collections)
        design.addFields(fields)
        self.assertEqual(design.fields, fields)
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN