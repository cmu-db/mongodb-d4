# -*- coding: utf-8 -*-

import os, sys
import time
import random
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
sys.path.append(os.path.join(basedir, "../../src"))

# Third-Party Dependencies
import mongokit

# mongodb-d4
import catalog
import workload
from util import constants
from inputs.abstractconverter import AbstractConverter

COLLECTION_NAME = "squirrels"
NUM_SESSIONS = 100
NUM_OPS_PER_SESSION = 4
NUM_FIELDS = 6

class TestAbstractConverter(unittest.TestCase):

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

        self.converter = AbstractConverter(self.metadata_db, self.dataset_db)
    ## DEF

    def testAddQueryHashes(self):
        # First make sure that all of the operations' query hashes are null
        for sess in self.metadata_db.Session.find():
            self.assertNotEqual(0, len(sess['operations']))
            for op in sess['operations']:
                self.assertIsNone(op['query_hash'])
        ## FOR

        # Now add the hashes. They should all be the same
        self.converter.addQueryHashes()
        firstHash = None
        for sess in self.metadata_db.Session.find():
            self.assertNotEqual(0, len(sess['operations']))
            for op in sess['operations']:
                if not firstHash:
                    firstHash = op['query_hash']
                    self.assertIsNotNone(firstHash)
                self.assertEqual(firstHash, op['query_hash'])
        ## FOR

    ## DEF

    def testExtractSchema(self):
        """
            Check whether we can successfully extract the database schema
            into our internal catalog
        """
        self.reconstructor.reconstructDatabase()
        self.reconstructor.extractSchema()

        col = self.metadata_db.Collection.one({"name": COLLECTION_NAME})
        # Add one for the '_id' field
        self.assertEqual(NUM_FIELDS + 1, len(col['fields']))
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN