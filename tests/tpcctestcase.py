
import os, sys
import random
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

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

sys.path.append(os.path.join(basedir, "../exps/benchmarks/"))
sys.path.append(os.path.join(basedir, "../exps/benchmarks/tpcc/"))
from tpcc import constants
from tpcc.runtime.executor import Executor
from tpcc.runtime import scaleparameters

class TPCCTestCase(MongoDBTestCase):
    """
        Base test case that will preload a TPCC-line workload
    """
    
    ALL_TRANSACTIONS = [
        constants.TransactionTypes.DELIVERY,
        constants.TransactionTypes.NEW_ORDER,
        constants.TransactionTypes.ORDER_STATUS,
        constants.TransactionTypes.PAYMENT,
        constants.TransactionTypes.STOCK_LEVEL,
    ]
    
    NUM_WAREHOUSES = 4
    SCALEFACTOR = 1
    NUM_SESSIONS = 10000

    def setUp(self):
        MongoDBTestCase.setUp(self)

        self.timestamp = time.time()
        self.query_id = 0
        self.resp_id = 0
        
        sp = scaleparameters.makeWithScaleFactor(TPCCTestCase.NUM_WAREHOUSES, TPCCTestCase.SCALEFACTOR)
        executor = Executor(sp)
        
        # WORKLOAD
        for i in xrange(TPCCTestCase.NUM_SESSIONS):
            sess = self.metadata_db.Session()
            sess['session_id'] = i
            sess['ip_client'] = "client:%d" % (1234+i)
            sess['ip_server'] = "server:5678"
            sess['start_time'] = self.timestamp
            
            txn, params = executor.doOne()
            if constants.TransactionTypes.DELIVERY == txn:
                sess['operations'] = self.createDelivery(params)
            elif constants.TransactionTypes.NEW_ORDER == txn:
                sess['operations'] = self.createNewOrder(params)
            elif constants.TransactionTypes.ORDER_STATUS == txn:
                sess['operations'] = self.createOrderStatus(params)
            elif constants.TransactionTypes.PAYMENT == txn:
                sess['operations'] = self.createPayment(params)
            elif constants.TransactionTypes.STOCK_LEVEL == txn:
                sess['operations'] = self.createStockLevel(params)
            else:
                assert False, "Unexpected TransactionType: " + txn

            print txn, params
            

            #for j in xrange(0, len(TPCCTestCase.COLLECTION_NAMES)):
                #_id = str(random.random())
                #queryId = long((i<<16) + j)
                #queryContent = { }
                #queryPredicates = { }

                #responseContent = {"_id": _id}
                #responseId = (queryId<<8)
                #for f in xrange(0, TPCCTestCase.NUM_FIELDS):
                    #f_name = "field%02d" % f
                    #if f % 2 == 0:
                        #responseContent[f_name] = random.randint(0, 100)
                        #queryContent[f_name] = responseContent[f_name]
                        #queryPredicates[f_name] = constants.PRED_TYPE_EQUALITY
                    #else:
                        #responseContent[f_name] = str(random.randint(1000, 100000))
                        ### FOR

                #queryContent = { constants.REPLACE_KEY_DOLLAR_PREFIX + "query": queryContent }
                #op = Session.operationFactory()
                #op['collection']    = TPCCTestCase.COLLECTION_NAMES[j]
                #op['type']          = constants.OP_TYPE_QUERY
                #op['query_id']      = queryId
                #op['query_content'] = [ queryContent ]
                #op['resp_content']  = [ responseContent ]
                #op['resp_id']       = responseId
                #op['predicates']    = queryPredicates
                #op['query_time']    = timestamp
                #timestamp += 1
                #op['resp_time']    = timestamp
                #sess['operations'].append(op)
                ### FOR (ops)
                
            sess['end_time'] = self.nextTimestamp(2)
            sess.save()
        ## FOR (sess)

        # Use the MongoSniffConverter to populate our metadata
        converter = MongoSniffConverter(self.metadata_db, self.dataset_db)
        converter.no_mongo_parse = True
        converter.no_mongo_sessionizer = True
        converter.process()
        self.assertEqual(TPCCTestCase.NUM_SESSIONS, self.metadata_db.Session.find().count())

        self.collections = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])
        self.assertEqual(len(TPCCTestCase.COLLECTION_NAMES), len(self.collections))

        populated_workload = list(c for c in self.metadata_db.Session.fetch())
        self.workload = populated_workload
        # Increase the database size beyond what the converter derived from the workload
        for col_name, col_info in self.collections.iteritems():
            col_info['doc_count'] = TPCCTestCase.NUM_DOCUMENTS
            col_info['avg_doc_size'] = 1024 # bytes
            col_info['max_pages'] = col_info['doc_count'] * col_info['avg_doc_size'] / (4 * 1024)
            col_info.save()
        #            print pformat(col_info)

    ## DEF
    
    def nextTimestamp(self, delta=1):
        self.timestamp += delta
        return self.timestamp
    def nextQueryId(self):
        self.query_id += 1
        return self.query_id
    def nextResponseId(self):
        self.resp_id += 1
        return self.resp_id
    
    def createNewOrder(self, params):
        pass
    ## DEF
    
    def createDelivery(self, params):
        pass
    ## DEF
    
    def createOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        ops = [ ]
        
        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_CUSTOMER
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id} ]
        op['query_fields']  = {"C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_ORDERS
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id} ]
        op['query_fields']  = {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_ORDER_LINE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id} ]
        op['query_fields']  = {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        return ops
    ## DEF
    
    def createStockLevel(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        o_id = random.randint(10000)
        ol_ids = [ random.randint(1000) for i in xrange(10) ]
        threshold = params["threshold"]
        ops = [ ]
        
        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"D_W_ID": w_id, "D_ID": d_id} ]
        op['query_fields']  = {"D_NEXT_O_ID": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_ORDER_LINE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"#lt": o_id, "#gte": o_id-20}} ]
        op['query_fields']  = {"OL_I_ID": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)

        op = Session.operationFactory()
        op['collection']    = constants.TABLENAME_STOCK
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ {"S_W_ID": w_id, "S_I_ID": {"$in": list(ol_ids)}, "S_QUANTITY": {"$lt": threshold}} ]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        return ops
    ## DEF
    
    def createPayment(self, params):
        pass
    ## DEF
    
    def testIgnore(self):
        pass
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN