#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import unittest
from pprint import pprint

import logging
logging.basicConfig(level = logging.INFO,
    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S",
    stream = sys.stdout)

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
sys.path.append(os.path.join(basedir, "../../src"))

import catalog
from search import InitialDesigner
from util import constants

COLLECTIONS = [ 'squirrels', 'girls' ]
FIELDS_PER_COLLECTION = 6

class TestDesign (unittest.TestCase):

    def setUp(self):
        self.collections = [ ]
        for col_name in COLLECTIONS:
            col_info = catalog.Collection()
            col_info['name'] = col_name
            col_info['workload_queries'] = random.randint(1000, 2000)

            for i in xrange(0, FIELDS_PER_COLLECTION):
                f_name = "field%02d" % i
                f_type = catalog.fieldTypeToString(int)
                f = catalog.Collection.fieldFactory(f_name, f_type)

                if i % 2 == 0:
                    f['query_use_count'] = col_info['workload_queries']
                    col_info['interesting'].append(f_name)
                col_info['fields'][f_name] = f
            ## FOR (fields)
            self.collections.append(col_info)
            pprint(col_info)
        ## FOR (collections)
    ## DEF

    def testGenerate(self):
        designer = InitialDesigner(self.collections)
        d = designer.generate()
        self.assertIsNotNone(d)

        # For each collection, the 'interesting' fields should
        # have been selected as the indexes and sharding keys
        # We don't need to worry about denormalization
        for col_info in self.collections:
            sharding = d.getShardKeys(col_info['name'])
            print "SHARDING:", sharding
            print "INTERESTING:", sorted(col_info['interesting'])
            self.assertListEqual(sorted(col_info['interesting']), sorted(sharding))

            indexes = d.getIndexes(col_info['name'])
            self.assertListEqual(sorted(col_info['interesting']), sorted(indexes[0]))
        ## FOR

    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN