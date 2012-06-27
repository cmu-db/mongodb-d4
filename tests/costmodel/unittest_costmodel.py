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
from inputs.mongodb import MongoSniffConverter

COLLECTION_NAMES = ["squirrels", "girls"]
NUM_DOCUMENTS = 10000
NUM_SESSIONS = 50
NUM_FIELDS = 6
NUM_NODES = 8

class TestCostModel(MongoDBTestCase):

    def setUp(self):
        MongoDBTestCase.setUp(self)

        # WORKLOAD
        self.workload = [ ]
        for i in xrange(0, NUM_SESSIONS):
            sess = self.metadata_db.Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = time.time()
            sess['end_time'] = time.time() + 5
            for j in xrange(0, len(COLLECTION_NAMES)):
                _id = str(random.random())
                queryId = long((i<<16) + j)
                queryContent = { }
                queryPredicates = { }

                responseContent = {"_id": _id}
                responseId = (queryId<<8)
                for f in xrange(0, NUM_FIELDS):
                    f_name = "field%02d" % f
                    if f % 2 == 0:
                        responseContent[f_name] = random.randint(0, 100)
                        queryContent[f_name] = responseContent[f_name]
                        queryPredicates[f_name] = constants.PRED_TYPE_EQUALITY
                    else:
                        responseContent[f_name] = str(random.randint(1000, 100000))
                    ## FOR

                queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query": queryContent }
                op = Session.operationFactory()
                op['collection']    = COLLECTION_NAMES[j]
                op['type']          = constants.OP_TYPE_QUERY
                op['query_id']      = queryId
                op['query_content'] = [ queryContent ]
                op['resp_content']  = [ responseContent ]
                op['resp_id']       = responseId
                op['predicates']    = queryPredicates
                sess['operations'].append(op)
            ## FOR (ops)
            sess.save()
            self.workload.append(sess)
        ## FOR (sess)

        # Use the MongoSniffConverter to populate our metadata
        converter = MongoSniffConverter(self.metadata_db, self.dataset_db)
        converter.no_mongo_parse = True
        converter.no_mongo_sessionizer = True
        converter.process()
        self.assertEqual(NUM_SESSIONS, self.metadata_db.Session.find().count())

        self.collections = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])
        self.assertEqual(len(COLLECTION_NAMES), len(self.collections))

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
        col_info = self.collections[COLLECTION_NAMES[0]]
        self.assertTrue(col_info['interesting'])

        # If we shard the collection on the interesting fields, then
        # each query should only need to touch one node
        d = Design()
        d.addCollection(col_info['name'])
        d.addShardKey(col_info['name'], col_info['interesting'])
        cost0 = self.cm.networkCost(d)
        print "cost0:", cost0

        # If we now shard the collection on just '_id', then every query
        # should have to touch every node. The cost of this design
        # should be greater than the first one
        d = Design()
        d.addCollection(col_info['name'])
        d.addShardKey(col_info['name'], ['_id'])
        cost1 = self.cm.networkCost(d)
        print "cost1:", cost1

        self.assertLess(cost0, cost1)
    ## DEF

#    def testNetworkCostDenormalization(self):
#        """Check network cost for queries that reference denormalized collections"""
#        pass
#    ## DEF

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