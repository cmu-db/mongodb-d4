#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
from tests import MongoDBTestCase
import catalog
import costmodel
from search import Design
from workload import Session
from util import constants

COLLECTION_NAME = "squirrels"
NUM_DOCUMENTS = 10000
NUM_SESSIONS = 100
NUM_OPS_PER_SESSION = 4
NUM_FIELDS = 6
NUM_NODES = 8

class TestCostModel(MongoDBTestCase):

    def setUp(self):
        MongoDBTestCase.setUp(self)

        # COLLECTIONS
        col_info = catalog.Collection()
        col_info['name'] = COLLECTION_NAME
        col_info['workload_percent'] = 1.0
        col_info['workload_queries'] = NUM_SESSIONS * NUM_OPS_PER_SESSION
        self.collections = {col_info['name']: col_info}

        for f in xrange(0, NUM_FIELDS):
            f_name = "field%02d" % f
            if f % 2 == 0:
                f_type = int
                col_info['interesting'].append(f_name)
                query_count = col_info['workload_queries']
            else:
                f_type = str
                queryCount = 0
            f = catalog.Collection.fieldFactory(f_name, catalog.fieldTypeToString(f_type))
            f['query_use_count'] = query_count
            col_info['fields'][f_name] = f
        ## FOR

        # WORKLOAD
        self.workload = [ ]
        for i in xrange(0, NUM_SESSIONS):
            sess = Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = time.time()
            sess['end_time'] = time.time() + 5
            for j in xrange(0, NUM_OPS_PER_SESSION):
                _id = str(random.random())
                queryId = long((i<<16) + j)
                queryContent = { }

                responseContent = {"_id": _id}
                responseId = (queryId<<8)
                for f in xrange(0, NUM_FIELDS):
                    f_name = "field%02d" % f
                    if f % 2 == 0:
                        responseContent[f_name] = random.randint(0, 100)
                        queryContent[f_name] = responseContent[f_name]
                    else:
                        responseContent[f_name] = str(random.randint(1000, 100000))
                    ## FOR

                queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query": queryContent }
                op = Session.operationFactory()
                op['collection']    = COLLECTION_NAME
                op['type']          = constants.OP_TYPE_QUERY
                op['query_id']      = queryId
                op['query_content'] = [ queryContent ]
                op['resp_content']  = [ responseContent ]
                op['resp_id']       = responseId
                op['predicates']    = dict([(f_name, constants.PRED_TYPE_EQUALITY) for f_name in  col_info['interesting']])
                sess['operations'].append(op)
            ## FOR (ops)

            self.workload.append(sess)
            ## FOR (sess)
        self.assertEqual(NUM_SESSIONS, len(self.workload))

        self.costModelConfig = {
           'max_memory':     6144, # MB
           'skew_intervals': 10,
           'address_size':   64,
           'nodes':          NUM_NODES,
        }
        self.cm = costmodel.CostModel(self.collections, self.workload, self.costModelConfig)
#        self.d = search.Design.testFactory()
    ## DEF

#    @staticmethod
#    def designFactory():
#        design = Design()
#        collections = ['col 1', 'col 2']
#        design.addCollections(collections)
#        design.addShardKey('col 1', ['c1b'])
#        design.addShardKey('col 2', ['c2a'])
#        design.addIndexes({ 'col 1' : [['c1a']], 'col 2' : [['c2c'], ['c2a', 'c2d']] })
#        return design
    
    def testNetworkCost(self):
        """Check network cost for equality predicate queries"""
        col_info = self.collections[COLLECTION_NAME]

        # If we shard the collection on the interesting fields, then
        # each query should only need to touch one node
        d = Design()
        d.addCollection(COLLECTION_NAME)
        d.addShardKey(COLLECTION_NAME, col_info['interesting'])
        cost0 = self.cm.networkCost(d)
        print "cost0:", cost0

        # If we now shard the collection on just '_id', then every query
        # should have to touch every node. The cost of this design
        # should be greater than the first one
        d = Design()
        d.addCollection(COLLECTION_NAME)
        d.addShardKey(COLLECTION_NAME, ['_id'])
        cost1 = self.cm.networkCost(d)
        print "cost1:", cost1

        self.assertLess(cost0, cost1)
    ## DEF

    def testNetworkCostDenormalization(self):
        """Check network cost for queries that reference denormalized collections"""
        pass
    ## DEF

#    def testDiskCost(self):
#        cost = self.cm.diskCost(self.d, self.w)
#        self.assertEqual(cost, 1.0)
#
#    def testSkewCost(self):
#        cost = self.cm.skewCost(self.d, self.w)
#        self.assertEqual(cost, 1.0)
#
#    def testOverallCost(self):
#        config = {'nodes' : 4}
#        cost = self.cm.overallCost(self.d, self.w, config)
#        self.assertEqual(cost, 0.0)
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN