# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest
from datetime import datetime
from pprint import pprint, pformat

import catalog

class TestUtilMethods(unittest.TestCase):
    
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
    
    def testSimpleExtractFields(self):
        doc = {
            'int':     123,
            'str':     'abc',
            'float':   123.4,
            # TODO
            #'list':    range(10),
            #'dict': ....
        }
        
        fields = { }
        catalog.extractFields(doc, fields)
        self.assertFalse(fields == None)
        self.assertEquals(dict, type(fields))
        for key, val in doc.items():
            self.assertTrue(key in fields)
            
            f = fields[key]
            self.assertFalse(f == None)
            self.assertEquals(key, f['type'])
        ## FOR
    ## DEF
  
    def testExtractFields(self):
        doc = {
            "similar_artists" : [
                    "50e130f676d6081483d7aeaf90702caa/7",
                    "3b6fac3e5e112ae35414480ccc5eb154/23",
            ],
            "name" : "596ea227ea0ce4dadbca2f06bddd30c9/15",
            "created" : {
                    "\$date" : 1335871184519l,
            },
            "image" : {
                    "large" : "1b942d952ccd004325c997c012d49354/49",
                    "extralarge" : "bd11cf67bd8ee7653a1cfdf782c4ffaa/49",
                    "small" : "f5728a43a9e3efac9a0670cc66c2229f/48",
                    "medium" : "00b6d53c70a4fe656a4fc867ed9aceed/48",
                    "mega" : "6998e2abb589312f0fd358943865bf3c/61"
            },
            "last_modified" : {
                    "\$date" : datetime.now(),
            },
            "alias_md5s" : [
                    "2b763d64b83180c5512a962d5c4d5115/34"
            ],
            "aliases" : [
                    "3019b6686229c4cf5089431332dee196/15"
            ]
        }
        
        fields = { }
        catalog.extractFields(doc, fields)
        self.assertNotEqual(fields, None)
        self.assertNotEqual(len(fields), 0)
        pprint(fields)
        
        # Check to make sure that we have a field entry for each
        # key in our original document. We will need to check recursively
        # to make sure that our nested keys get picked up
        for key, val in doc.iteritems():
            self.assertTrue(key in fields, key)
            f = fields[key]
            self.assertNotEqual(f, None)
        ## FOR
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN