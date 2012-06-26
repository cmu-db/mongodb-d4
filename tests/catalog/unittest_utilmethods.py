# -*- coding: utf-8 -*-

import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest

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
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN