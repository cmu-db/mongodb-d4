
import os, sys
import random
import time
import string
import unittest
from pprint import pformat

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))
sys.path.append(os.path.join(basedir, "../exps/benchmarks/"))
sys.path.append(os.path.join(basedir, "../exps/benchmarks/tpcc/"))
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
from tpcc import constants as tpccConstants
from tpcc.runtime.executor import Executor
from tpcc.runtime import scaleparameters

class TPCCTestCase(MongoDBTestCase):
    """
        Base test case that will preload a TPCC-line workload
    """
    
    ALL_TRANSACTIONS = [
        tpccConstants.TransactionTypes.DELIVERY,
        tpccConstants.TransactionTypes.NEW_ORDER,
        tpccConstants.TransactionTypes.ORDER_STATUS,
        tpccConstants.TransactionTypes.PAYMENT,
        tpccConstants.TransactionTypes.STOCK_LEVEL,
    ]
    
    NUM_WAREHOUSES = 4
    SCALEFACTOR = 1
    NUM_SESSIONS = 50

    def setUp(self):
        MongoDBTestCase.setUp(self)

        random.seed(0) # Needed for TPC-C code
        self.rng = random.Random(0)
        self.timestamp = time.time()
        self.query_id = 0l
        self.resp_id = 0l
        
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
            if tpccConstants.TransactionTypes.DELIVERY == txn:
                sess['operations'] = self.createDelivery(params)
            elif tpccConstants.TransactionTypes.NEW_ORDER == txn:
                sess['operations'] = self.createNewOrder(params)
            elif tpccConstants.TransactionTypes.ORDER_STATUS == txn:
                sess['operations'] = self.createOrderStatus(params)
            elif tpccConstants.TransactionTypes.PAYMENT == txn:
                sess['operations'] = self.createPayment(params)
            elif tpccConstants.TransactionTypes.STOCK_LEVEL == txn:
                sess['operations'] = self.createStockLevel(params)
            else:
                assert False, "Unexpected TransactionType: " + txn

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
        
        populated_workload = list(c for c in self.metadata_db.Session.fetch())
        self.workload = populated_workload
        
        # Increase the database size beyond what the converter derived from the workload
        for col_name, col_info in self.collections.iteritems():
            col_info['doc_count'] = 10000
            col_info['avg_doc_size'] = 1024 # bytes
            col_info['max_pages'] = col_info['doc_count'] * col_info['avg_doc_size'] / (4 * 1024)
            for k,v in col_info['fields'].iteritems():
                if col_name == tpccConstants.TABLENAME_ORDER_LINE:
                    v['parent_col'] = tpccConstants.TABLENAME_ORDERS
            col_info.save()
            # print pformat(col_info)
            
        self.costModelConfig = {
            'max_memory':     1024, # MB
            'skew_intervals': 10,
            'address_size':   64,
            'nodes':          10,
            'window_size':    10
        }

                    
        self.state = State(self.collections, populated_workload, self.costModelConfig)
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
        ops = [ ]
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "S_DIST_%02d" % d_id
        w_tax = self.rng.random()
        d_tax = self.rng.random()
        d_next_o_id = self.rng.randint(0, 1000)
        c_discount = self.rng.randint(0, 10)
        ol_cnt = len(i_ids)
        o_carrier_id = tpccConstants.NULL_CARRIER_ID
        all_local = (not i_w_ids or [w_id] * len(i_w_ids) == i_w_ids)
        
        op = Session.operationFactory()
        responseContent = {"I_ID": self.rng.randint(0, 100)}
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_ITEM
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"I_ID": {"#in": i_ids}}}]
        op['query_fields']  = {"I_ID": 1, "I_PRICE": 1, "I_NAME": 1, "I_DATA": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()

        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_WAREHOUSE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"W_ID": w_id}}]
        op['query_fields']  = {"W_TAX": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
                
        op = Session.operationFactory()
        responseContent = {}
        responseContent["D_ID"] = self.rng.randint(0, 100)
        responseContent["D_W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"D_ID": d_id, "D_W_ID": w_id}}]
        op['query_fields']  = {"D_TAX": 1, "D_NEXT_O_ID": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["D_ID"] = self.rng.randint(0, 100)
        responseContent["D_W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_UPDATE
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"D_ID": d_id, "D_W_ID": w_id}, {"#inc": {"D_NEXT_O_ID": 1}}]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        op['update_multi']  = False
        op['update_upsert'] = True
        ops.append(op)
                
        op = Session.operationFactory()
        responseContent = {}
        responseContent["C_ID"] = self.rng.randint(0, 100)
        responseContent["C_D_ID"] = self.rng.randint(0, 100)
        responseContent["C_W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_CUSTOMER
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}}]
        op['query_fields']  = {"C_DISCOUNT": 1, "C_LAST": 1, "C_CREDIT": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["NO_O_ID"] = self.rng.randint(0, 100)
        responseContent["NO_D_ID"] = self.rng.randint(0, 100)
        responseContent["NO_W_ID"] = self.rng.randint(0, 100)

        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_NEW_ORDER
        op['type']          = constants.OP_TYPE_INSERT
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        o = {
            "O_D_ID": d_id,
            "O_W_ID": w_id,
            "O_C_ID": c_id,
            "O_ID": d_next_o_id,
            "O_ENTRY_D": o_entry_d,
            "O_CARRIER_ID": o_carrier_id,
            "O_OL_CNT": ol_cnt,
            "O_ALL_LOCAL": all_local
        }
        responseContent = {
            "O_D_ID": self.rng.randint(0, 100),
            "O_W_ID": self.rng.randint(0, 100),
            "O_C_ID": self.rng.randint(0, 100),
            "O_ID": self.rng.randint(0, 100),
            "O_ENTRY_D": self.rng.randint(0, 100),
            "O_CARRIER_ID": self.rng.randint(0, 100),
            "O_OL_CNT": self.rng.randint(0, 100),
            "O_ALL_LOCAL": self.rng.randint(0, 100)
        }

        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_ORDERS
        op['type']          = constants.OP_TYPE_INSERT
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [o]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
                
        op = Session.operationFactory()
        responseContent = {}
        responseContent["S_I_ID"] = self.rng.randint(0, 100)
        responseContent["S_W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_STOCK
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"S_I_ID": {"#in": i_ids}, "S_W_ID": w_id}}]
        op['query_fields']  = {"S_I_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        for i in range(ol_cnt):
            s = {"S_I_ID": i_ids[i], "S_W_ID": w_id}
            ol = {
                "OL_D_ID": d_id,
                "OL_W_ID": w_id,
                "OL_O_ID": d_next_o_id,
                "OL_NUMBER": i + 1,
                "OL_I_ID": i_ids[i],
                "OL_SUPPLY_W_ID": i_w_ids[i],
                "OL_DELIVERY_D": o_entry_d,
                "OL_QUANTITY": i_qtys[i],
                "OL_AMOUNT": self.rng.random() * 100,
                "OL_DIST_INFO": ''.join(self.rng.choice(string.ascii_uppercase) for x in range(24))
            }
            s_remote_cnt = self.rng.randint(0, 10)
            s_order_cnt = self.rng.randint(0, 10)
            s_quantity = self.rng.randint(0, 10)
            s_ytd = self.rng.random()
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["S_I_ID"] = self.rng.randint(0, 100)
            responseContent["S_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_STOCK
            op['type']          = constants.OP_TYPE_UPDATE
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [s, {"#set": {"S_QUANTITY": s_quantity, "S_YTD": s_ytd, "S_ORDER_CNT": s_order_cnt, "S_REMOTE_CNT": s_remote_cnt}}]
            op['query_fields']  = None
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            op['update_upsert'] = True
            ops.append(op)
        ## FOR
        return ops
    ## DEF
    
    def createDelivery(self, params):
        ops = [ ]
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        for d_id in xrange(1, tpccConstants.DISTRICTS_PER_WAREHOUSE+1):
            c_id = self.rng.randint(0, 10000)
            o_id = self.rng.randint(0, 10000)
            ol_total = self.rng.random() * 100
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["NO_D_ID"] = self.rng.randint(0, 100)
            responseContent["NO_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_NEW_ORDER
            op['type']          = constants.OP_TYPE_QUERY
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"#query" : {"NO_D_ID": d_id, "NO_W_ID": w_id}}]
            op['query_fields']  = {"NO_O_ID": 1}
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["O_ID"] = self.rng.randint(0, 100)
            responseContent["O_D_ID"] = self.rng.randint(0, 100)
            responseContent["O_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_ORDERS
            op['type']          = constants.OP_TYPE_QUERY
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"#query" :  {"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id}}]
            op['query_fields']  = {"O_C_ID": 1}
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["OL_O_ID"] = self.rng.randint(0, 100)
            responseContent["OL_D_ID"] = self.rng.randint(0, 100)
            responseContent["OL_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_ORDER_LINE
            op['type']          = constants.OP_TYPE_QUERY
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"#query" : {"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}}]
            op['query_fields']  = {"OL_AMOUNT": 1}
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["O_ID"] = self.rng.randint(0, 100)
            responseContent["O_D_ID"] = self.rng.randint(0, 100)
            responseContent["O_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_ORDERS
            op['type']          = constants.OP_TYPE_UPDATE
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"#set": {"O_CARRIER_ID": o_carrier_id}} ]
            op['query_fields']  = None
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            op['update_multi']  = False
            op['update_upsert'] = True
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["OL_O_ID"] = self.rng.randint(0, 100)
            responseContent["OL_D_ID"] = self.rng.randint(0, 100)
            responseContent["OL_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_ORDER_LINE
            op['type']          = constants.OP_TYPE_UPDATE
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"#set": {"OL_DELIVERY_D": ol_delivery_d}}]
            op['query_fields']  = None
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            op['update_multi']  = True
            op['update_upsert'] = True
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["C_ID"] = self.rng.randint(0, 100)
            responseContent["C_D_ID"] = self.rng.randint(0, 100)
            responseContent["C_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_CUSTOMER
            op['type']          = constants.OP_TYPE_UPDATE
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"#inc": {"C_BALANCE": ol_total}}]
            op['query_fields']  = None
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            op['update_multi']  = False
            op['update_upsert'] = True
            ops.append(op)
            
            op = Session.operationFactory()
            responseContent = {}
            responseContent["NO_D_ID"] = self.rng.randint(0, 100)
            responseContent["NO_W_ID"] = self.rng.randint(0, 100)
            op['resp_content']  = [responseContent]
            op['collection']    = tpccConstants.TABLENAME_NEW_ORDER
            op['type']          = constants.OP_TYPE_DELETE
            op['query_id']      = self.nextQueryId()
            op['query_content'] = [{"NO_D_ID": d_id, "NO_W_ID": w_id}]
            op['query_fields']  = None
            op['resp_id']       = self.nextResponseId()
            op['query_time']    = self.nextTimestamp()
            op['resp_time']     = self.nextTimestamp()
            ops.append(op)
        ## FOR
        return ops
    ## DEF
    
    def createOrderStatus(self, params):
        ops = [ ]
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        o_id = self.rng.randint(0, 10000)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["C_W_ID"] = self.rng.randint(0, 100)
        responseContent["C_D_ID"] = self.rng.randint(0, 100)
        responseContent["C_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_CUSTOMER
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id}}]
        op['query_fields']  = {"C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["O_W_ID"] = self.rng.randint(0, 100)
        responseContent["O_D_ID"] = self.rng.randint(0, 100)
        responseContent["O_C_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_ORDERS
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id}}]
        op['query_fields']  = {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["OL_W_ID"] = self.rng.randint(0, 100)
        responseContent["OL_D_ID"] = self.rng.randint(0, 100)
        responseContent["OL_O_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_ORDER_LINE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id}}]
        op['query_fields']  = {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        return ops
    ## DEF
    
    def createStockLevel(self, params):
        ops = [ ]
        w_id = params["w_id"]
        d_id = params["d_id"]
        o_id = self.rng.randint(0, 10000)
        ol_ids = [ self.rng.randint(0, 1000) for i in xrange(10) ]
        threshold = params["threshold"]
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["D_W_ID"] = self.rng.randint(0, 100)
        responseContent["D_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"D_W_ID": w_id, "D_ID": d_id}}]
        op['query_fields']  = {"D_NEXT_O_ID": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["OL_W_ID"] = self.rng.randint(0, 100)
        responseContent["OL_D_ID"] = self.rng.randint(0, 100)
        responseContent["OL_O_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_ORDER_LINE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"#lt": o_id, "#gte": o_id-20}}}]
        op['query_fields']  = {"OL_I_ID": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)

        op = Session.operationFactory()
        responseContent = {}
        responseContent["S_W_ID"] = self.rng.randint(0, 100)
        responseContent["S_I_ID"] = self.rng.randint(0, 100)
        responseContent["S_QUANTITY"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_STOCK
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"S_W_ID": w_id, "S_I_ID": {"#in": list(ol_ids)}, "S_QUANTITY": {"#lt": threshold}}}]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        return ops
    ## DEF
    
    def createPayment(self, params):
        ops = [ ]
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["C_W_ID"] = self.rng.randint(0, 100)
        responseContent["C_D_ID"] = self.rng.randint(0, 100)
        responseContent["C_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_CUSTOMER
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id}}]
        op['query_fields']  = {"C_BALANCE": 0, "C_YTD_PAYMENT": 0, "C_PAYMENT_CNT": 0}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["W_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_WAREHOUSE
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"W_ID": w_id}}]
        op['query_fields']  = {"W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["W_NAME"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_WAREHOUSE
        op['type']          = constants.OP_TYPE_UPDATE
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [ { "W_NAME" : "igmrhawo" }, { "#inc" : { "W_YTD" : 123 }} ]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        op['update_upsert'] = True
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["D_W_ID"] = self.rng.randint(0, 100)
        responseContent["D_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"D_W_ID": w_id, "D_ID": d_id}}]
        op['query_fields']  = {"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1}
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        op = Session.operationFactory()
        responseContent = {}
        responseContent["D_ID"] = self.rng.randint(0, 100)
        op['resp_content']  = [responseContent]
        op['collection']    = tpccConstants.TABLENAME_DISTRICT
        op['type']          = constants.OP_TYPE_QUERY
        op['query_id']      = self.nextQueryId()
        op['query_content'] = [{"#query" : {"D_ID": d_id}}, {"#inc": {"D_YTD": h_amount}} ]
        op['query_fields']  = None
        op['resp_id']       = self.nextResponseId()
        op['query_time']    = self.nextTimestamp()
        op['resp_time']     = self.nextTimestamp()
        ops.append(op)
        
        return ops
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN