# -*- coding: utf-8 -*-

import unittest

import catalog

class testUtilMethods(unittest.TestCase):
    
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
    
    def testExtractFields(self):
        doc = {
            'int':     123,
            'str':     'abc',
            'float':   123.4,
            # TODO
            #'list':    range(10),
            #'dict': ....
        }
        
        fields = catalog.extractFields(doc)
        self.assertFalse(fields == None)
        self.assertEquals(dict, type(fields))
        for key, val in doc.items():
            self.assertTrue(key in fields)
            
            f = fields[key]
            self.assertFalse(f == None)
            self.assertEquals(key, f['type'])
        ## FOR
    ## DEF
        
        
## CLASS