#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import string
import random
import unittest
from pprint import pprint, pformat

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
from util.histogram import Histogram

class TestHistogram(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def testPickle(self):
        h = Histogram()
        letters = [ x for x in string.letters ] + ["-"]
        
        for i in xrange(0, 100):
            key = ""
            for x in xrange(0, 10):
                key += random.choice(letters)
            assert len(key) > 0
            
            h.put(key, delta=random.randint(1, 10))
            assert h[key] > 0
        ## FOR
        
        # Serialize
        import pickle
        p = pickle.dumps(h, -1)
        assert p
        
        # Deserialize
        clone = pickle.loads(p)
        assert clone
        
        for key in h.keys():
            self.assertEquals(h[key], clone[key])
        ## FOR
        self.assertEquals(h.getSampleCount(), clone.getSampleCount())
        self.assertEquals(sorted(h.getMinCountKeys()), sorted(clone.getMinCountKeys()))
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN