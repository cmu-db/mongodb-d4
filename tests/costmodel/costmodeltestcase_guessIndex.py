import os, sys
import random
import time

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))
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

    COLLECTION_NAME = "apple"
    NUM_DOCUMENTS = 10000000
    NUM_SESSIONS = 1
    NUM_FIELDS = 1
    NUM_NODES = 1
    NUM_INTERVALS = 10

    def setUp(self):
        MongoDBTestCase.setUp(self)

        # WORKLOAD
        timestamp = time.time()

        sess = self.metadata_db.Session()
        sess['session_id'] = 0
        sess['ip_client'] = "client:%d" % (1234+0)
        sess['ip_server'] = "server:5678"
        sess['start_time'] = timestamp

        # generate query 0 querying field00
        _id = str(random.random())
        queryId = long((0<<16) + 0)
        queryContent = { }
        queryPredicates = { }
        projectionField = { }

        responseContent = {"_id": _id}
        responseId = (queryId<<8)

        responseContent['field00'] = random.randint(0, 100)
        queryContent['field00'] = responseContent['field00']
        queryPredicates['field00'] = constants.PRED_TYPE_EQUALITY
        projectionField['field02'] = random.randint(0, 100)

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
        op['query_fields']   = projectionField
        timestamp += 1
        op['resp_time']    = timestamp

        sess['operations'].append(op)

        # generate query 1 querying field01
        _id = str(random.random())
        queryId = long((1<<16) + 1)
        queryContent = { }
        queryPredicates = { }

        responseContent = {"_id": _id}
        responseId = (queryId<<8)
        projectionField = { }

        responseContent['field01'] = random.randint(0, 100)
        queryContent['field01'] = responseContent['field01']
        queryPredicates['field01'] = constants.PRED_TYPE_EQUALITY
        projectionField['field02'] = random.randint(0, 100)

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
        op['query_fields']   = projectionField
        timestamp += 1
        op['resp_time']    = timestamp

        sess['operations'].append(op)

        # generate query 2 querying field00, field01
        _id = str(random.random())
        queryId = long((2<<16) + 2)
        queryContent = { }
        queryPredicates = { }
        projectionField = { }

        responseContent = {"_id": _id}
        responseId = (queryId<<8)

        responseContent['field01'] = random.randint(0, 100)
        queryContent['field01'] = responseContent['field01']
        queryPredicates['field01'] = constants.PRED_TYPE_EQUALITY

        responseContent['field00'] = random.randint(0, 100)
        queryContent['field00'] = responseContent['field01']
        queryPredicates['field00'] = constants.PRED_TYPE_EQUALITY

        projectionField['field02'] = random.randint(0, 100)

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
        op['query_fields']   = projectionField
        timestamp += 1
        op['resp_time']    = timestamp

        sess['operations'].append(op)

        sess['end_time'] = timestamp
        timestamp += 1

        # generate query 2 querying field00, field01 but without projection field
        _id = str(random.random())
        queryId = long((2<<16) + 3)
        queryContent = { }
        queryPredicates = { }
        projectionField = { }

        responseContent = {"_id": _id}
        responseId = (queryId<<8)

        responseContent['field01'] = random.randint(0, 100)
        queryContent['field01'] = responseContent['field01']
        queryPredicates['field01'] = constants.PRED_TYPE_EQUALITY

        responseContent['field00'] = random.randint(0, 100)
        queryContent['field00'] = responseContent['field01']
        queryPredicates['field00'] = constants.PRED_TYPE_EQUALITY

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
        op['query_fields']   = projectionField
        timestamp += 1
        op['resp_time']    = timestamp

        sess['operations'].append(op)

        sess['end_time'] = timestamp
        timestamp += 1

        sess.save()

        # Use the MongoSniffConverter to populate our metadata
        converter = MongoSniffConverter(self.metadata_db, self.dataset_db)
        converter.no_mongo_parse = True
        converter.no_mongo_sessionizer = True
        converter.process()
        self.assertEqual(CostModelTestCase.NUM_SESSIONS, self.metadata_db.Session.find().count())

        self.collections = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])

        populated_workload = list(c for c in self.metadata_db.Session.fetch())
        self.workload = populated_workload
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
            'window_size':    10
        }

        self.state = State(self.collections, populated_workload, self.costModelConfig)
    ## DEF
## CLASS
