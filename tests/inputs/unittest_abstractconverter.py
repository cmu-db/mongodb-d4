# -*- coding: utf-8 -*-

import os, sys
import time
import random
import unittest
from datetime import datetime
from pprint import pprint

import logging
logging.basicConfig(level = logging.INFO,
    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S",
    stream = sys.stdout)

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
sys.path.append(os.path.join(basedir, "../../src"))

# Third-Party Dependencies
import mongokit

# mongodb-d4
import catalog
import workload
from util import constants
from inputs.mongodb.reconstructor import Reconstructor
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

                responseContent = {"_id": _id}
                responseId = (queryId<<8)
                for f in xrange(0, NUM_FIELDS):
                    f_name = "field%02d" % f
                    if f % 2 == 0:
                        responseContent[f_name] = random.randint(0, 100)
                    else:
                        responseContent[f_name] = str(random.randint(1000, 100000))
                ## FOR

                queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query": responseContent }
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

    def testPostProcess(self):
        """
            Check whether we can successfully extract the database schema
            into our internal catalog
        """
        Reconstructor(self.metadata_db, self.dataset_db).process()
        self.converter.postProcess()

        col_info = self.metadata_db.Collection.one({"name": COLLECTION_NAME})
#        pprint(col_info)

        # Workload-derived Attributes
        self.assertEqual(NUM_SESSIONS*NUM_OPS_PER_SESSION, col_info['workload_queries'])
        self.assertAlmostEqual(1.0, col_info['workload_percent'])

        # Fields
        # Add one for the '_id' field count
        self.assertEqual(NUM_FIELDS + 1, len(col_info['fields']))
        for k,field in col_info['fields'].iteritems():
            self.assertEqual(NUM_SESSIONS*NUM_OPS_PER_SESSION, field['query_use_count'])

    ## DEF

    def testProcessDataFieldsSimple(self):
        doc = {
            'int':     123,
            'str':     'abc',
            'float':   123.4,
            # TODO
            #'list':    range(10),
            #'dict': ....
        }

        col_info = catalog.Collection()
        col_info['data_size'] = 0

        fields = { }
        self.converter.processDataFields(col_info, fields, doc)
        self.assertIsNotNone(fields)
        self.assertEquals(dict, type(fields))
        for key, val in doc.iteritems():
            self.assertIn(key, fields)
            f = fields[key]
            self.assertIsNotNone(f)
            self.assertEquals(key, f['type'])
        ## FOR
    ## DEF

    def testProcessDataFields(self):
        doc = {
            "similar_artists" : [
                "50e130f676d6081483d7aeaf90702caa/7",
                "3b6fac3e5e112ae35414480ccc5eb154/23",
                ],
            "name" : "596ea227ea0ce4dadbca2f06bddd30c9/15",
            "created" : {
                "\$date" : 1335871184519l,
                },
            "image" : {
                "large" : "1b942d952ccd004325c997c012d49354/49",
                "extralarge" : "bd11cf67bd8ee7653a1cfdf782c4ffaa/49",
                "small" : "f5728a43a9e3efac9a0670cc66c2229f/48",
                "medium" : "00b6d53c70a4fe656a4fc867ed9aceed/48",
                "mega" : "6998e2abb589312f0fd358943865bf3c/61"
            },
            "last_modified" : {
                "\$date" : datetime.now(),
                },
            "alias_md5s" : [
                "2b763d64b83180c5512a962d5c4d5115/34"
            ],
            "aliases" : [
                "3019b6686229c4cf5089431332dee196/15"
            ]
        }

        col_info = catalog.Collection()
        col_info['data_size'] = 0

        fields = { }
        self.converter.processDataFields(col_info, fields, doc)
        self.assertIsNotNone(fields)
        self.assertNotEqual(len(fields), 0)
#        pprint(fields)

        # Check to make sure that we have a field entry for each
        # key in our original document. We will need to check recursively
        # to make sure that our nested keys get picked up
        for key, val in doc.iteritems():
            self.assertIn(key, fields)
            f = fields[key]
            self.assertNotEqual(f, None)
        ## FOR
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN