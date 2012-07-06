#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from costmodeltestcase import CostModelTestCase
from search import Design
from workload import Session
from util import constants
from costmodel.skew import SkewCostComponent

class TestSkewCost(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)
        self.cm = SkewCostComponent(self.state)
    ## DEF

    def testSkewCost(self):
        """Check whether skew cost calculations work correctly"""
        col_info = self.collections[CostModelTestCase.COLLECTION_NAMES[0]]
        shard_key = col_info['interesting'][0]

        d = Design()
        d.addCollection(col_info['name'])
        d.addShardKey(col_info['name'], [shard_key])

        # First get the skew cost when the queries got each node uniformly
        # This is the best-case scenario
        op_ctr = 0
        for sess in self.workload:
            for op in sess['operations']:
                query_content = [ {constants.REPLACE_KEY_DOLLAR_PREFIX + "query":\
                           {shard_key: op_ctr % CostModelTestCase.NUM_NODES }\
                } ]
                op['collection'] = col_info['name']
                op['query_content'] = query_content
                op['predicates'] = { shard_key: constants.PRED_TYPE_EQUALITY }
                op_ctr += 1
            ## FOR (op)
        ## FOR (session)
        cost0 = self.cm.getCost(d)
        self.assertLessEqual(cost0, 1.0)
        #        print "skewCost0:", cost0

        # Then make all of the operations go to a single node
        # This is the worst-case scenario
        query_content = [ {constants.REPLACE_KEY_DOLLAR_PREFIX + "query":\
                                   {shard_key: 1000l }\
        } ]
        for sess in self.workload:
            for op in sess['operations']:
                op['collection'] = col_info['name']
                op['query_content'] = query_content
                op['predicates'] = { shard_key: constants.PRED_TYPE_EQUALITY }
            ## FOR
        self.state.reset()
        self.cm.reset()
        cost1 = self.cm.getCost(d)
        self.assertLessEqual(cost1, 1.0)
        #        print "skewCost1:", cost1

        self.assertGreater(cost1, cost0)

    ## DEF

    def testGetSplitWorkload(self):
        """Check that the workload is split into intervals"""

        self.assertEqual(CostModelTestCase.NUM_SESSIONS, sum(map(len, self.cm.workload_segments)))
        for i in xrange(0, CostModelTestCase.NUM_INTERVALS):
        #            print "[%02d]: %d" % (i, len(self.cm.workload_segments[i]))
            self.assertGreater(len(self.cm.workload_segments[i]), 0)
        ## FOR
        self.assertEqual(CostModelTestCase.NUM_INTERVALS, len(self.cm.workload_segments))
    ## DEF


## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN