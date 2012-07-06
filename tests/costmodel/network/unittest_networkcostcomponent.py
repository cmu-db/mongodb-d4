#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import random
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../"))

# mongodb-d4
try:
    from mongodbtestcase import MongoDBTestCase
except ImportError:
    from tests import MongoDBTestCase
import catalog
import costmodel
from search import Design
from workload import Session
from util import constants
from inputs.mongodb import MongoSniffConverter

COLLECTION_NAMES = ["squirrels", "girls"]
NUM_DOCUMENTS = 10000000
NUM_SESSIONS = 100
NUM_FIELDS = 6
NUM_NODES = 8
NUM_INTERVALS = 10

class TestNetworkCostComponent(MongoDBTestCase):

    def setUp(self):
        MongoDBTestCase.setUp(self)

        # WORKLOAD
        self.workload = [ ]
        timestamp = time.time()
        for i in xrange(0, NUM_SESSIONS):
            sess = self.metadata_db.Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = timestamp

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

                op['query_time']    = timestamp
                timestamp += 1
                op['resp_time']    = timestamp

                sess['operations'].append(op)
            ## FOR (ops)
            sess['end_time'] = timestamp
            timestamp += 2
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

        # Increase the database size beyond what the converter derived from the workload
        for col_name, col_info in self.collections.iteritems():
            col_info['doc_count'] = NUM_DOCUMENTS
            col_info['avg_doc_size'] = 1024 # bytes
            col_info['max_pages'] = col_info['doc_count'] * col_info['avg_doc_size'] / (4 * 1024)
            col_info.save()
#            print pformat(col_info)

        # Setup CostModel
        self.costModelConfig = {
           'max_memory':     1024, # MB
           'skew_intervals': NUM_INTERVALS,
           'address_size':   64,
           'nodes':          NUM_NODES,
        }
        self.cm = costmodel.CostModel(self.collections, self.workload, self.costModelConfig)
    ## DEF

    def testDiskCost(self):
        """Check whether disk cost calculations work correctly"""

        # First get the disk cost when there are no indexes
        d = Design()
        for i in xrange(len(COLLECTION_NAMES)):
            col_info = self.collections[COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        ## FOR
        cost0 = self.cm.diskCost(d)
        print "diskCost0:", cost0
        # The cost should be exactly equal to one, which means that every operation
        # has to perform a full sequential scan on the collection
        self.assertEqual(cost0, 1.0)

        # Now add the indexes. The disk cost should be lower
        d = Design()
        for i in xrange(len(COLLECTION_NAMES)):
            col_info = self.collections[COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            d.addIndex(col_info['name'], col_info['interesting'])
            self.cm.invalidateCache(col_info['name'])
        ## FOR
        cost1 = self.cm.diskCost(d)
        print "diskCost1:", cost1
        self.assertGreater(cost0, cost1)
    ## DEF

    def testDiskCostCaching(self):
        """Check whether disk cost calculations work correctly with caching enabled"""
        self.cm.cache_enable = True

        # Give the mofo a full Design with indexes
        d = Design()
        for i in xrange(len(COLLECTION_NAMES)):
            col_info = self.collections[COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
            d.addIndex(col_info['name'], col_info['interesting'])
        ## FOR
        cost0 = self.cm.diskCost(d)
        print "diskCost0:", cost0
        self.assertGreater(cost0, 0.0)

        # We should get the same cost back after we execute it a second time
        cost1 = self.cm.diskCost(d)
        print "diskCost1:", cost1
        self.assertEqual(cost0, cost1)
    ## DEF

    def testEstimateWorkingSets(self):
        """Check the working set size estimator for collections"""

        d = Design()
        for i in xrange(0, len(COLLECTION_NAMES)):
            col_info = self.collections[COLLECTION_NAMES[i]]
            d.addCollection(col_info['name'])
        ## FOR

        max_memory = self.costModelConfig['max_memory'] * 1024 * 1024
        workingSets = self.cm.estimateWorkingSets(d, max_memory)
        self.assertIsNotNone(workingSets)

        for i in xrange(0, len(COLLECTION_NAMES)):
            col_info = self.collections[COLLECTION_NAMES[i]]
            self.assertIn(col_info['name'], workingSets)
            setSize = workingSets[col_info['name']]
            print col_info['name'], "->", setSize
            self.assertGreater(setSize, 0.0)
    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN