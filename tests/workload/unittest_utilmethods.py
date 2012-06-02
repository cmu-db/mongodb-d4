#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append("/home/pavlo/Documents/MongoDB/MongoDB-Designer/src")

import unittest
from workload import utilmethods
from pprint import pprint, pformat

class TestUtilMethods (unittest.TestCase):
    
    def setUp(self):
        pass
    
    def getAllKeys(self, d, keys=None):
        if keys == None: keys = []
        for k, v in d.iteritems():
            if not k in keys: keys.append(k)
            if type(v) == dict:
                self.getAllKeys(v, keys)
        return keys
    ## DEF
    
    def testEscapeFieldNames(self):
        content = [
            {'$query': {'_id': '1cba73b8a555ba442a3630ccf735dffd/14'}},
            {'$query': {'_id': {'$in': []}}},
            {'count': '107f3bf172abf9dae6458f1dbb0d4ad6/11',
             'fields': None,
             'query': {'md5': {'$in': ['c3117f341b734d3ce6e71608480de82d/34']}}},
        ]
        
        for i in xrange(0, len(content)):
            orig = content[i]
            
            escaped = utilmethods.escapeFieldNames(content[i])
            self.assertNotEqual(escaped, None)
            keys = self.getAllKeys(escaped)
            for k in keys:
                self.assertFalse(k.startswith('$'), pformat(escaped))
            print pformat(escaped)
        ## FOR
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN