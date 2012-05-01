#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append("/home/pavlo/Documents/MongoDB/MongoDB-Designer/src")

import unittest
from workload.ophasher import OpHasher

class TestOpHasher (unittest.TestCase):
    
    def setUp(self):
        self.hasher = OpHasher()
        pass
    
    def genQuery(self, query):
        return [ {"query": query} ]
        
    def testHashQuery01(self):
        op = {
            "collection": u'ABC',
            "content":    self.genQuery({"a": 2}),
            "type":       "$query",
        }
        h0 = self.hasher.hash(op)
        self.assertNotEqual(h0, None)

        op["content"] = self.genQuery({"a": 3})
        h1 = self.hasher.hash(op)
        self.assertEqual(h0, h1)
        
        op["content"] = self.genQuery({"a": {"$all": [2, 3]}})
        h2 = self.hasher.hash(op)
        self.assertNotEqual(h0, h2)
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN