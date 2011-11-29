# -*- coding: utf-8 -*-

import unittest

import catalog

class testFieldType(unittest.TestCase):
    
    def setUp(self):
        self.fieldType = catalog.FieldType()
    
    def testSerialization(self):
        for t in [ int, str, unicode, float ]:
            t_bson = self.fieldType.to_bson(t)
            self.assertFalse(t_bson == None)
            #print "BSON:", t_bson
            t_python = self.fieldType.to_python(t_bson)
            self.assertFalse(t_python == None)
            #print "PYTHON:", t_python
            self.assertEquals(t, t_python)
        ## FOR
    ## DEF
        
## CLASS

#if __name__ == '__main__':
    #unittest.main()