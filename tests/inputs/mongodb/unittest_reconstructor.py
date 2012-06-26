#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import time
import random
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../libs"))
sys.path.append(os.path.join(basedir, "../../../src"))

# Third-Party Dependencies
import mongokit

# mongodb-d4
import catalog
import workload
from util import constants
from inputs.mongodb.reconstructor import Reconstructor

COLLECTION_NAME = "squirrels"
NUM_SESSIONS = 100
NUM_OPS_PER_SESSION = 4
NUM_FIELDS = 6

'''
{
    "resp_id" : 108573252,
    "query_time" : 1338410624.76486,
    "query_limit" : -1,
    "resp_content" : [
            {
                    "website" : "e4e0848c0213e9ec35378357c072bc4e/22",
                    "bio" : "b23e46b797cb9604ac10ffd0b5d1fd8d/38",
                    "_id" : "4e284300507afcc25589043fec3ce46e/7",
                    "preferences" : {
                            "new_follower_email" : true,
                            "relove_email" : true,
                            "newsletter" : true,
                            "weekly_recap_email" : true
                    }
            }
    ],
    "query_aggregate" : false,
    "collection" : "exfm.user.meta",
    "resp_time" : 1338410624.764927,
    "query_id" : 1470946406,
    "query_hash" : NumberLong("5967555224994160467"),
    "query_offset" : 0,
    "query_size" : 76,
    "resp_size" : 893,
    "type" : "$query",
    "query_content" : [
            {
                    "#query" : {
                            "_id" : "4e284300507afcc25589043fec3ce46e/7"
                    }
            }
    ]
},
'''
class TestReconstructor(unittest.TestCase):

    def setUp(self):
        conn = mongokit.Connection()
        conn.register([ catalog.Collection, workload.Session ])

        # Drop the databases first
        # Note that we prepend "test_" in front of the db names
        db_prefix = "test_"
        for dbName in [constants.METADATA_DB_NAME, constants.DATASET_DB_NAME]:
            conn.drop_database(db_prefix + dbName)
        self.metadata_db = conn[db_prefix + constants.METADATA_DB_NAME]
        self.dataset_db = conn[db_prefix + constants.DATASET_DB_NAME]

        # Generate some fake workload sessions
        for i in xrange(0, NUM_SESSIONS):
            sess = self.metadata_db.Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = time.time()
            sess['end_time'] = time.time() + 5
            for j in xrange(0, NUM_OPS_PER_SESSION):
                _id = str(random.random())
                queryId = (i<<16) + j
                queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query":
                                 { "_id": [ _id ] } }

                responseContent = {"_id": _id}
                responseId = (queryId<<8)
                for f in xrange(0, NUM_FIELDS):
                    f_name = "field%02d" % f
                    if f % 2 == 0:
                        responseContent[f_name] = random.randint(0, 100)
                    else:
                        responseContent[f_name] = str(random.randint(1000, 100000))
                ## FOR

                op = workload.Session.operationFactory()
                op['collection']    = COLLECTION_NAME
                op['type']          = constants.OP_TYPE_QUERY
                op['query_id']      = queryId
                op['query_content'] = [ queryContent ]
                op['resp_content']  = [ responseContent ]
                op['resp_id']       = responseId
                sess['operations'].append(op)
            ## FOR (ops)

            sess.save()
        ## FOR (sess)
        self.assertEqual(NUM_SESSIONS, self.metadata_db.Session.find().count())

        self.reconstructor = Reconstructor(self.metadata_db, self.dataset_db)
    ## DEF

    def testProcess(self):
        """
            Check whether the reconstructed database includes the fields
            that were returned in the query responses
        """
        self.reconstructor.process()

        fields = [ "field%02d" % f for f in xrange(0, NUM_FIELDS) ]
        num_docs = 0
        for doc in self.dataset_db[COLLECTION_NAME].find():
            # pprint(doc)
            for f in fields:
                self.assertIn(f, doc.keys())
            num_docs += 1
        # We should always have one per operation
        self.assertEquals(num_docs, NUM_SESSIONS * NUM_OPS_PER_SESSION)
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN