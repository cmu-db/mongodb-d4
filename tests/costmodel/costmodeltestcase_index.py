import os, sys
from random import shuffle
import random
import time

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
try:
    from mongodbtestcase import MongoDBTestCase
except ImportError:
    from tests import MongoDBTestCase

from costmodel.state import State
from search import Design
from workload import Session
from util import constants
from inputs.mongodb import MongoSniffConverter

class CostModelTestCase(MongoDBTestCase):
    """
        Base test case for cost model components
    """

    COLLECTION_NAME = "apples"
    NUM_DOCUMENTS = 10000000
    NUM_SESSIONS = 100
    NUM_FIELDS = 2
    NUM_NODES = 1
    NUM_INTERVALS = 10

    def setUp(self):
        MongoDBTestCase.setUp(self)

        # WORKLOAD
        timestamp = time.time()
        # add queries for collection 0 querying field00
        for i in  xrange(CostModelTestCase.NUM_SESSIONS):
            sess = self.metadata_db.Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = timestamp

            _id = str(random.random())
            queryId = long((i<<16) + 0)
            queryContent = { }
            queryPredicates = { }

            responseContent = {"_id": _id}
            responseId = (queryId<<8)
            
            if i < 80:
                f_name_target = "field00"
                f_name_miss = "field01"
            else:
                f_name_target = "field01"
                f_name_miss = "field00"
            
            responseContent[f_name_target] = str(random.randint(1000, 100000))
            responseContent[f_name_target] = random.randint(0, 100)
            queryContent[f_name_target] = responseContent[f_name_target]
            queryPredicates[f_name_target] = constants.PRED_TYPE_EQUALITY
            responseContent[f_name_miss] = str(random.randint(1000, 100000))

            queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query": queryContent }
            op = Session.operationFactory()
            op['collection']    = CostModelTestCase.COLLECTION_NAME
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
        ## FOR (sess)
        
        # Use the MongoSniffConverter to populate our metadata
        converter = MongoSniffConverter(self.metadata_db, self.dataset_db)
        converter.no_mongo_parse = True
        converter.no_mongo_sessionizer = True
        converter.process()
        self.assertEqual(CostModelTestCase.NUM_SESSIONS, self.metadata_db.Session.find().count())

        self.collections = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])
        self.assertEqual(1, len(self.collections))

        populated_workload = list(c for c in self.metadata_db.Session.fetch())
        shuffle(populated_workload)

        # Increase the database size beyond what the converter derived from the workload
        for col_name, col_info in self.collections.iteritems():
            col_info['doc_count'] = CostModelTestCase.NUM_DOCUMENTS
            col_info['avg_doc_size'] = 1024 # bytes
            col_info['max_pages'] = col_info['doc_count'] * col_info['avg_doc_size'] / (4 * 1024)
            col_info.save()
            #            print pformat(col_info)

        self.costModelConfig = {
            'max_memory':     1024, # MB
            'skew_intervals': CostModelTestCase.NUM_INTERVALS,
            'address_size':   64,
            'nodes':          CostModelTestCase.NUM_NODES,
            'window_size':    2
        }

        self.state = State(self.collections, populated_workload, self.costModelConfig)
        ## DEF
## CLASS
