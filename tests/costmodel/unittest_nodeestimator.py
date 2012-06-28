#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
try:
    from mongodbtestcase import MongoDBTestCase
except ImportError:
    from tests import MongoDBTestCase

import catalog
from costmodel import NodeEstimator
from search import Design
from workload import Session
from util import constants
from inputs.mongodb import MongoSniffConverter

COLLECTION_NAMES = ["squirrels", "girls"]
NUM_DOCUMENTS = 1000000
NUM_SESSIONS = 1
NUM_FIELDS = 6
NUM_NODES = 8
NUM_INTERVALS = 10

class TestNodeEstimator(MongoDBTestCase):

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

        self.estimator = NodeEstimator(NUM_NODES)
    ## DEF


#    def testEstimateOp(self):
#        """Check the estimating touched nodes for a simple op"""
#
#        d = Design()
#        for i in xrange(0, len(COLLECTION_NAMES)):
#            col_info = self.collections[COLLECTION_NAMES[i]]
#            d.addCollection(col_info['name'])
#            # Only put the first field in the interesting list as the
#            # the sharding key. We'll worry about compound sharding
#            # keys later.
#            d.addShardKey(col_info['name'], col_info['interesting'][:1])
#        ## FOR
#
#        sess = self.metadata_db.Session.fetch_one()
#        op = sess['operations'][0]
#
#        touched = self.estimator.estimateOp(d, op)
#    ## DEF

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN