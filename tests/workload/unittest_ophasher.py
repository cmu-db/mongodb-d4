#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest

from util import constants
from workload.ophasher import OpHasher

class TestOpHasher (unittest.TestCase):
    
    def setUp(self):
        self.hasher = OpHasher()
        pass
    
    def genQuery(self, query):
        return [ {constants.REPLACE_KEY_DOLLAR_PREFIX + "query": query} ]
        
    def genUpdate(self, query, update):
        return [ query, update ]
        
    def testHashQuery(self):
        op = {
            "collection":       u'ABC',
            "query_content":    self.genQuery({"a": 2}),
            "type":             "$query",
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
    
    def testComplexQuery(self):
        content =  {u'_id': u'7df2cdb0268fe84ad602e228d75f4812/108',
                     u'cid': {u'#oid': u'310794ef49b9b02c7f29b1ff64c6f7b3/26'},
                     u'd': u'b34918b94d030d5b288053f08258f1c9/10',
                     u'g': u'5e3f1e67d663a535fe0ceeab07dd0e12/12',
                     u'hid': u'd259f04f68e37fdebff7c55b67a04fb7/34',
                     u'hy': {u'0': {u'n': 0, u't': 0},
                             u'1': {u'n': 0, u't': 0},
                             u'10': {u'n': 0, u't': 0},
                             u'11': {u'n': 0, u't': 0},
                             u'12': {u'n': 0, u't': 0},
                             u'13': {u'n': 0, u't': 0},
                             u'14': {u'n': 0, u't': 0},
                             u'15': {u'n': 0, u't': 0},
                             u'16': {u'n': 0, u't': 0},
                             u'17': {u'n': 0, u't': 0},
                             u'18': {u'n': 0, u't': 0},
                             u'19': {u'n': 0, u't': 0},
                             u'2': {u'n': 0, u't': 0},
                             u'20': {u'n': 0, u't': 0},
                             u'21': {u'n': 0, u't': 0},
                             u'22': {u'n': 0, u't': 0},
                             u'23': {u'n': 0, u't': 0},
                             u'3': {u'n': 0, u't': 0},
                             u'4': {u'n': 0, u't': 0},
                             u'5': {u'n': 0, u't': 0},
                             u'6': {u'n': 0, u't': 0},
                             u'7': {u'n': 0, u't': 0},
                             u'8': {u'n': 0, u't': 0},
                             u'9': {u'n': 0, u't': 0}},
                     u'i': u'22922d9f495e1502e3af3dac1a8a4a8b/22'}
        op = {
            "collection":       u'ABC',
            "query_content":    self.genQuery(content),
            "type":             "$query",
        }
        h0 = self.hasher.hash(op)
        self.assertNotEqual(h0, None)
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