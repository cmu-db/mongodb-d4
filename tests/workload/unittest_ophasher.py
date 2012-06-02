#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from workload.ophasher import OpHasher

class TestOpHasher (unittest.TestCase):
    
    def setUp(self):
        self.hasher = OpHasher()
        pass
    
    def genQuery(self, query):
        return [ {"query": query} ]
        
    def genUpdate(self, query, update):
        return [ query, update ]
        
    def testHashQuery(self):
        op = {
            "collection": u'ABC',
            "query_content":    self.genQuery({"a": 2}),
            "type":       "$query",
        }
        h0 = self.hasher.hash(op)
        self.assertNotEqual(h0, None)

        op["query_content"] = self.genQuery({"a": 3})
        h1 = self.hasher.hash(op)
        self.assertEqual(h0, h1)
        
        op["query_content"] = self.genQuery({"a": {"$all": [2, 3]}})
        h2 = self.hasher.hash(op)
        self.assertNotEqual(h0, h2)
    ## DEF
    
    def testHashUpdate(self):
        whereClause = {"u_id": 123, "i_id": 456}
        updateClause = {"rating": 999}
        
        op = {
            "collection": u'ABC',
            "query_content":    self.genUpdate(whereClause, updateClause),
            "type":       "$update",
        }
        h0 = self.hasher.hash(op)
        self.assertNotEqual(h0, None)
        
        newWhere = dict(whereClause.items() + [("XXX", 123)])
        op["query_content"] = self.genUpdate(newWhere, updateClause)
        h1 = self.hasher.hash(op)
        self.assertNotEqual(h0, h1)
        
        newUpdate = dict(updateClause.items() + [("XXX", 123)])
        op["query_content"] = self.genUpdate(whereClause, newUpdate)
        h2 = self.hasher.hash(op)
        self.assertNotEqual(h0, h2)
        ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN