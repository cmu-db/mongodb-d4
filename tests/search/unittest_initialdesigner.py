#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import unittest
import logging
from pprint import pprint

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from tpcctestcase import TPCCTestCase
from search import Design
from workload import Session
import catalog
from search import InitialDesigner
from util import constants, configutil

class TestInitialDesigner(TPCCTestCase):

    def setUp(self):
        TPCCTestCase.setUp(self)
        self.config = configutil.makeDefaultConfig()
        self.designer = InitialDesigner(self.collections, self.workload, self.config)
        self.col_keys = self.designer.generateCollectionHistograms()
        self.design = Design()
        map(self.design.addCollection, self.col_keys.iterkeys())
    ## DEF
    
    def testCheckForInvalidKeys(self):
        d = self.designer.generate()
        self.assertIsNotNone(d)
        
        # Make sure that we don't have any invalid keys
        for col_name in d.getCollections():
            for index_keys in d.getIndexes(col_name):
                for key in index_keys:
                    assert not key.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX), \
                        "Invalid index key '%s.%s'" % (col_name, key)
                ## FOR
            for key in d.getShardKeys(col_name):
                assert not key.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX), \
                    "Invalid shard key '%s.%s'" % (col_name, key)
        ## FOR
    ## DEF

    def testSelectShardingKeys(self):
        # Select on set of keys at random and increase its occurence
        # in the histogram so that we will pick it
        expected = { }
        for col_name, h in self.col_keys.iteritems():
            keys = random.choice(h.keys())
            h.put(keys, 999999)
            expected[col_name] = keys
        
        self.designer.__selectShardingKeys__(self.design, self.col_keys)
        
        # Then check to make sure it picked what we expected it to
        for col_name in self.col_keys.iterkeys():
            shard_keys = self.design.getShardKeys(col_name)
            self.assertIsNotNone(shard_keys)
            self.assertIsInstance(shard_keys, tuple)
            self.assertEquals(expected[col_name], shard_keys)
        #print self.design
    ## DEF
    
    def testSelectIndexKeys(self):
        # Select on set of keys at random and increase its occurence
        # in the histogram so that we will pick it
        expected = { }
        for col_name, h in self.col_keys.iteritems():
            keys = random.choice(h.keys())
            h.put(keys, 999999)
            expected[col_name] = keys
        
        node_memory = self.config.get(configutil.SECT_CLUSTER, "node_memory")
        self.designer.__selectIndexKeys__(self.design, self.col_keys, node_memory)
        #print self.design
        
        # Then check to make sure it picked what we expected it to
        for col_name in self.col_keys.iterkeys():
            index_keys = self.design.getIndexKeys(col_name)
            self.assertIsNotNone(index_keys)
            self.assertIsInstance(index_keys, list)
            # FIXME self.assertEquals(expected[col_name], shard_keys)
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN