#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import string
import random
import unittest
from pprint import pprint, pformat

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../exps"))
from api.results import Results

class TestResults(unittest.TestCase):
    
    def setUp(self):
        self.txnNames = [ ]
        for i in xrange(0, 6):
            self.txnNames.append("txn-%02d" % i)
        pass
    
    def compareResults(self, r1, r2):
        self.assertEquals(r1.start, r2.start)
        self.assertEquals(r1.stop, r2.stop)
        for txn in self.txnNames:
            self.assertEquals(r1.txn_counters[txn], r2.txn_counters[txn])
            self.assertEquals(r1.txn_times[txn], r2.txn_times[txn])
        ## FOR
        self.assertEquals(len(r1.completed), len(r2.completed))
    ## DEF
    
    def testOpCount(self):
        totalOpCount = 0
        results = [ Results() for i in xrange(10)  ]
        map(Results.startBenchmark, results)
        for r in results:
            for i in xrange(0, 5000):
                txn = random.choice(self.txnNames)
                id = r.startTransaction(txn)
                assert id != None
                ops = random.randint(1, 10)
                r.stopTransaction(id, ops)
                totalOpCount += ops
            ## FOR
        ## FOR
        map(Results.stopBenchmark, results)
        
        r = Results()
        map(r.append, results)
        self.assertEquals(totalOpCount, r.opCount)
    ## DEF
        
    
    def testAppend(self):
        r1 = Results()
        r1.startBenchmark()
        for i in xrange(0, 5000):
            txn = random.choice(self.txnNames)
            id = r1.startTransaction(txn)
            assert id != None
            r1.stopTransaction(id, 1)
        ## FOR
        r1.stopBenchmark()
        print r1.show()
        
        # Append the time and then make sure they're the same
        r2 = Results()
        r2.append(r1)
        self.compareResults(r1, r2)
        
    ## DEF
    
    def testPickle(self):
        r = Results()
        r.startBenchmark()
        for i in xrange(0, 1000):
            txn = random.choice(self.txnNames)
            id = r.startTransaction(txn)
            assert id != None
            r.stopTransaction(id, 1)
        ## FOR
        
        # Serialize
        import pickle
        p = pickle.dumps(r, -1)
        assert p
        
        # Deserialize
        clone = pickle.loads(p)
        assert clone
        
        # Make sure the txn counts are equal
        self.compareResults(r, clone)
    ## DEF
    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN