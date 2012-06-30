# -*- coding: utf-8 -*-

import os, sys
import itertools

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest

import catalog

class TestUtilMethods(unittest.TestCase):
    TEST_FIELDS = {
        "scalarKey": 1234,
        "listKey":   range(10),
        "nestedKey": {
            "innerKey1": 5678,
            "innerKey2": 5678,
        }
    }


    def testGetAllValues(self):
        values = catalog.getAllValues(TestUtilMethods.TEST_FIELDS)
        self.assertIsNotNone(values)
        self.assertIsInstance(values, tuple)

        # Make sure we can hash it
        hash_v = hash(values)
#        print "hash_v:", hash_v
        self.assertIsNotNone(hash_v)

        for v in TestUtilMethods.TEST_FIELDS.itervalues():
            if isinstance(v, dict):
                expected = tuple(v.values())
            elif isinstance(v, list):
                expected = tuple(v)
            else:
                expected = v
            self.assertIn(expected, values)
        ## FOR
    ## DEF

    def testGetFieldValue(self):
        fields = TestUtilMethods.TEST_FIELDS
        for shardKey in fields.keys():
            expected = fields[shardKey]
            if shardKey == "nestedKey":
                expected = fields[shardKey]["innerKey2"]
                shardKey += ".innerKey2"

            actual = catalog.getFieldValue(shardKey, fields)
#            print shardKey, "->", actual
            self.assertIsNotNone(actual, shardKey)
            self.assertEqual(expected, actual, shardKey)
        ## FOR

        ## Make sure that if we give it an invald key that we get back None
        actual = catalog.getFieldValue("LiptonSoup", fields)
        self.assertIsNone(actual)
    ## DEF

    def testFieldTypeSerialization(self):
        for t in [ int, str, unicode, float ]:
            t_bson = catalog.fieldTypeToString(t)
            self.assertFalse(t_bson == None)
            #print "BSON:", t_bson
            t_python = catalog.fieldTypeToPython(t_bson)
            self.assertFalse(t_python == None)
            #print "PYTHON:", t_python
            self.assertEquals(t, t_python)
        ## FOR
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN