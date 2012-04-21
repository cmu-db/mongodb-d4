# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from datetime import datetime
import sys
sys.path.append(r'/opt/hypertable/current/lib/py')
sys.path.append(r'/opt/hypertable/current/lib/py/gen-py')
import constants
from abstractdriver import AbstractDriver

from hypertable.thriftclient import *
from hyperthrift.gen.ttypes import *
from time import time

import re

TXN_QUERIES = {
    'DELIVERY': {
        'getNewOrder': 'SELECT NO_O_ID FROM NEW_ORDER WHERE ROW =^ \"?\" CELL_LIMIT 1',
        'deleteNewOrder':'DELETE * FROM NEW_ORDER WHERE ROW = \"?\" ',
        'getCId': 'SELECT O_C_ID FROM ORDERS WHERE ROW = \"?\" CELL_LIMIT 1',
        'updateOrders':'INSERT INTO ORDERS VALUES (\"?\",\"O_CARRIER_ID\",\"?\")',
        'updateOrderLine':'INSERT INTO ORDER_LINE VALUES (\"?\",\"OL_DELIVERY_D\",\"?\")',
        'selectOrderLineKey': 'SELECT OL_O_ID FROM ORDER_LINE WHERE ROW =^ \"?\" KEYS_ONLY',
        'sumOLAmount': 'SELECT OL_AMOUNT FROM ORDER_LINE WHERE ROW =^ \"?\" CELL_LIMIT 1',
        'getCustomerBalance': 'SELECT C_BALANCE FROM CUSTOMER WHERE ROW = \"?\" CELL_LIMIT 1',
        'updateCustomerBalance': 'INSERT INTO CUSTOMER VALUES (\"?\", \"C_BALANCE\", \"?\")',
        },
    'NEW_ORDER': {
        'getWarehouseTaxRate':'SELECT W_TAX FROM WAREHOUSE WHERE ROW = \"?\" CELL_LIMIT 1', # rowkey
        'getDistrict': "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE ROW = \"?\" CELL_LIMIT 1", # rowkey
        'incrementNextOrderId': 'INSERT INTO DISTRICT VALUES (\"?\", \"D_NEXT_O_ID\", \"?\")', # rowkey, d_next_o_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE ROW = \"?\" CELL_LIMIT 1", # rowkey
        "createOrder": "INSERT INTO ORDERS VALUES (\"?\", \"O_ID\", \"?\"), (\"?\", \"O_D_ID\", \"?\"), (\"?\", \"O_W_ID\", \"?\"), (\"?\", \"O_C_ID\", \"?\"), (\"?\", \"O_ENTRY_D\",\"?\"),(\"?\", \"O_CARRIER_ID\", \"?\"), (\"?\", \"O_OL_CNT\", \"?\"), (\"?\", \"O_ALL_LOCAL\", \"?\")", # 
        "createNewOrder": "INSERT INTO NEW_ORDER VALUES (\"?\", \"NO_O_ID\", \"?\"), (\"?\", \"NO_D_ID\", \"?\"), (\"?\", \"NO_W_ID\", \"?\")", #
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE ROW = \"?\" CELL_LIMIT 1", #
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE ROW = \"?\" CELL_LIMIT 1", # 
        "updateStock": "INSERT INTO STOCK VALUES (\"?\", \"S_QUANTITY\", \"?\"), (\"?\", \"S_YTD\", \"?\"),(\"?\", \"S_ORDER_CNT\",\"?\"), (\"?\", \"S_REMOTE_CNT\", \"?\")", #
        "createOrderLine": "INSERT INTO ORDER_LINE VALUES (\"?\", \"OL_O_ID\",\"?\"),(\"?\", \"OL_D_ID\", \"?\"),(\"?\", \"OL_W_ID\", \"?\"),(\"?\", \"OL_NUMBER\", \"?\"),(\"?\", \"OL_I_ID\", \"?\"),(\"?\", \"OL_SUPPLY_W_ID\", \"?\"),(\"?\", \"OL_DELIVERY_D\", \"?\"),(\"?\", \"OL_QUANTITY\", \"?\"),(\"?\", \"OL_AMOUNT\", \"?\"),(\"?\" , \"OL_DIST_INFO\",\"?\")",
        },
     'ORDER_STATUS': {
        'getCustomerByCustomerId':'SELECT C_ID,C_FIRST,C_MIDDLE,C_LAST,C_BALANCE FROM CUSTOMER WHERE ROW = \"?\" CELL_LIMIT 1',
        'getCustomersWithoutCID': 'SELECT C_ID,C_FIRST,C_MIDDLE,C_LAST,C_BALANCE FROM CUSTOMER WHERE ROW =^ \"?\" CELL_LIMIT 1',
        'getOrdersWithoutOID': 'SELECT O_ID, O_CARRIER_ID, O_ENTRY_D  FROM ORDERS WHERE ROW =^ \"?\"',
        'getOrderLines':'SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE ROW =^ \"?\" ',
        },
    'PAYMENT':{
        'getWarehouse':'SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD FROM WAREHOUSE WHERE ROW = \"?\" ',# W_YTD is used in updateWarehouseBalance, not need in the original SQL
        'updateWarehouseBalance':'INSERT INTO WAREHOUSE VALUES (\"?\", \"W_YTD\", \"?\")',
        'getDistrict':'SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD FROM DISTRICT WHERE ROW = \"?\" ',# D_YTD is used in updateWarehouseBalance, not need in the original SQL
        'updateDistrictBalance':'INSERT INTO DISTRICT VALUES (\"?\", \"D_YTD\", \"?\")',
        'getCustomerByCustomerId':'SELECT C_ID,C_FIRST,C_MIDDLE,C_LAST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_PHONE,C_SINCE, C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE, C_YTD_PAYMENT,C_PAYMENT_CNT,C_DATA FROM CUSTOMER WHERE ROW = \"?\"',
        'getCustomersWithoutCID':'SELECT C_ID,C_FIRST,C_MIDDLE,C_LAST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE, C_YTD_PAYMENT,C_PAYMENT_CNT,C_DATA FROM CUSTOMER WHERE ROW =^ \"?\"',
        'updateBCCustomer':'INSERT INTO CUSTOMER VALUES (\"?\",\"C_BALANCE\",\"?\"),(\"?\",\"C_YTD_PAYMENT\",\"?\"),(\"?\",\"C_PAYMENT_CNT\",\"?\"),(\"?\",\"C_DATA\",\"?\")',
        'updateGCCustomer':'INSERT INTO CUSTOMER VALUES (\"?\",\"C_BALANCE\",\"?\"),(\"?\",\"C_YTD_PAYMENT\",\"?\"),(\"?\",\"C_PAYMENT_CNT\",\"?\")',
        'insertHistory':'INSERT INTO HISTORY VALUES (\"?\", \"H_C_ID\", \"?\"), (\"?\", \"H_C_D_ID\", \"?\"), (\"?\", \"H_C_W_ID\", \"?\"), (\"?\", \"H_D_ID\", \"?\"), (\"?\", \"H_W_ID\", \"?\"), (\"?\", \"H_DATE\", \"?\"), (\"?\", \"H_AMOUNT\", \"?\"), (\"?\", \"H_DATA\", \"?\")',
        },
    'STOCK_LEVEL':{
        'getOId':'SELECT D_NEXT_O_ID FROM DISTRICT WHERE ROW = \"?\" CELL_LIMIT 1 ',
        'getOrderLineInfo': 'SELECT OL_O_ID, OL_I_ID FROM ORDER_LINE WHERE ROW =^ \"?\" CELL_LIMIT 1',
        'getStockInfo': 'SELECT S_I_ID, S_QUANTITY FROM STOCK WHERE ROW =^ \"?\" CELL_LIMIT 1',
        }

    }

## ==============================================
## HypertableDriver
## ==============================================
class HypertableDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        'host': ('hostname', 'localhost'),
        'port': ('port', 38080),
        'namespace': ('namespace name', 'tpcc')
        }

    COLUMN_FAMILY_NAME = {
        'ITEM': ['I_ID','I_IM_ID','I_NAME','I_PRICE', 'I_DATA'],
        'WAREHOUSE':['W_ID','W_NAME','W_STREET_1','W_STREET_2',
                     'W_CITY','W_STATE','W_ZIP','W_TAX','W_YTD'],
        'DISTRICT':['D_ID','D_W_ID','D_NAME','D_STREET_1','D_STREET_2',
                    'D_CITY','D_STATE','D_ZIP','D_TAX','D_YTD','D_NEXT_O_ID'],
        'CUSTOMER':['C_ID','C_D_ID','C_W_ID','C_FIRST','C_MIDDLE','C_LAST',
                    'C_STREET_1','C_STREET_2','C_CITY','C_STATE','C_ZIP',
                    'C_PHONE','C_SINCE','C_CREDIT','C_CREDIT_LIM','C_DISCOUNT',
                    'C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT',
                    'C_DELIVERY_CNT','C_DATA'],
        'ORDERS':['O_ID','O_C_ID','O_W_ID','O_D_ID','O_ENTRY_D','O_CARRIER_ID',
                 'O_OL_CNT','O_ALL_LOCAL'],#O_C_ID and O_D_ID are switched
        'NEW_ORDER':['NO_O_ID','NO_D_ID','NO_W_ID'],
        'STOCK':['S_I_ID','S_W_ID','S_QUANTITY','S_DIST_01','S_DIST_02',
                 'S_DIST_03','S_DIST_04','S_DIST_05','S_DIST_06','S_DIST_07',
                 'S_DIST_08','S_DIST_09','S_DIST_10','S_YTD',
                 'S_ORDER_CNT','S_REMOTE_CNT','S_DATA'],
        'ORDER_LINE':['OL_O_ID','OL_D_ID','OL_W_ID','OL_NUMBER','OL_I_ID',
                      'OL_SUPPLY_W_ID','OL_DELIVERY_D','OL_QUANTITY',
                      'OL_AMOUNT','OL_DIST_INFO'],
        'HISTORY':['H_C_ID', 'H_C_D_ID', 'H_C_W_ID', 'H_D_ID', 'H_W_ID', 'H_DATE', 'H_AMOUNT', 'H_DATA']
        }

    ROW_KEY = {
        'ITEM': ['I_ID'],
        'WAREHOUSE' : ['W_ID'],
        'DISTRICT' : ['D_W_ID','D_ID'],
        'CUSTOMER' : ['C_W_ID','C_D_ID','C_ID'],
        'ORDERS': ['O_W_ID','O_D_ID','O_ID'],
        'NEW_ORDER':['NO_W_ID','NO_D_ID','NO_O_ID'],
        'STOCK':['S_W_ID','S_I_ID'],
        'ORDER_LINE':['OL_W_ID','OL_D_ID','OL_O_ID','OL_NUMBER'],
        'HISTORY':['H_C_ID']
        }

    KEY_LENGTH ={
        'I_ID':6,
        'S_I_ID':6,

        'W_ID':4,
        'D_W_ID':4,
        'C_W_ID':4,
        'O_W_ID':4,
        'NO_W_ID':4,
        'S_W_ID':4,
        'OL_W_ID':4,

        'D_ID':4,
        'C_D_ID':4,
        'O_D_ID':4,
        'NO_D_ID':4,
        'OL_D_ID':4,

        'C_ID':6,
        'H_C_ID':6,
        'O_C_ID':6,

        'O_ID':8,
        'NO_O_ID':8,
        'OL_O_ID':8,

        'OL_NUMBER':2,
        }

    def __init__(self, ddl):
        super(HypertableDriver, self).__init__('hypertable', ddl)
        self.client = None
        self.namespace = None


    def _getRowKey2(self,tablename,row):
        rowKey = ''
        TABLE = self.COLUMN_FAMILY_NAME
        for key in self.ROW_KEY[tablename]:
                k = str(row[TABLE[tablename].index(key)])
                k = '0' * (self.KEY_LENGTH[key] - len(k)) + k
                rowKey += k
        return rowKey

    # get rowkey from
    # (1) row tuple in order defined in COLUMN_FAMILY_NAME
    # (2) keys consisting the row key in order defined in ROW_KEY
    def _getRowKey(self,tablename, row):
        rowKey = ''
        TABLE = self.ROW_KEY

        for i in range(0,len(row)):
            k = str(row[i])
            k = '0' * (self.KEY_LENGTH[self.ROW_KEY[tablename][i]] - len(k)) + k
            rowKey += k
        #print tablename + ' ' + rowKey
        return rowKey

        # case (1)
        # if len(row) == len(self.COLUMN_FAMILY_NAME[tablename]):
        #     for key in self.ROW_KEY[tablename]:
        #         k = str(row[self.COLUMN_FAMILY_NAME[tablename].index(key)])
        #         k = '0' * (self.KEY_LENGTH[key] - len(k)) + k
        #         rowKey += k
        # case (2)
        # elif len(row) == len(self.ROW_KEY[tablename]):
        #     for key in self.ROW_KEY[tablename]:
        #         k = str(row[self.ROW_KEY[tablename].index(key)])
        #         k = '0' * (self.KEY_LENGTH[key] - len(k)) + k
        #         rowKey += k            

        return rowKey

    def _execute(self, hql, params):
        splits = re.split('\?', hql)
        hql = splits[0]
        assert len(splits) == len(params) + 1
        for i in range(0,len(params)):
            hql += str(params[i])
            hql += str(splits[i+1])
        return self.client.hql_exec(self.namespace, hql, 0, 0)

    def makeDefaultConfig(self):
        return HypertableDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        for key in HypertableDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        self.client = ThriftClient(config["host"], int(config["port"]))

        if not self.client.exists_namespace(config["namespace"]):
            try:
                self.client.create_namespace(config["namespace"])
            except Error:
                logging.error("Caughy exception when trying to create namespace")
                raise

        try:
            self.namespace = self.client.open_namespace(config["namespace"])
        except:
            logging.error("Caught exception when tyring to open namespace %s" % NAMESPACE_NAME)

    def loadTuples(self, tableName, tuples):

        if not self.client.exists_table(self.namespace, tableName):
            hql = 'create table ' + tableName + ' ('
            for column in self.COLUMN_FAMILY_NAME[tableName]:
                hql += column + ','
            hql = hql[:len(hql)-1]
            hql += ')'
            try:
                self.client.hql_query(self.namespace, hql)
            except:
                pass

        print len(tuples)
        columnFamilyName = self.COLUMN_FAMILY_NAME[tableName]

        cells = list()

        print tableName
        for row in tuples:
            rowKey = self._getRowKey2(tableName, row)
            #print columnFamilyName[0]
            #print str(row[0])

            for i in range(0,len(row)):
                cells.append(Cell(Key(rowKey, columnFamilyName[i], None),str(row[i])))
        print 'submit'
        self.client.offer_cells(self.namespace, tableName, MutateSpec("tpcc",10000,2), cells)

    def doDelivery(self, params):
        q = TXN_QUERIES["DELIVERY"]

        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            #rs = self.cursor.execute(q["getNewOrder"], [d_id, w_id])
            rowkey = self._getRowKey("NEW_ORDER", [w_id, d_id])
            rs = self._execute(q['getNewOrder'], [rowkey])
            if len(rs.cells) == 0:
                ## No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            for cell in rs.cells:
                if cell.key.column_family == 'NO_O_ID':
                    no_o_id = cell.value

            #self.cursor.execute(q["getCId"], [no_o_id, d_id, w_id])            
            rowkey = self._getRowKey("ORDERS", [w_id, d_id, no_o_id])
            rs = self._execute(q['getCId'], [rowkey])
            if len(rs.cells) == 0 :
                return
            for cell in rs.cells:
                if cell.key.column_family == 'O_C_ID':
                    c_id = cell.value
                    break;

            #self.cursor.execute(q["sumOLAmount"], [no_o_id, d_id, w_id])
            #ol_total = self.cursor.fetchone()[0]
            rowkey = self._getRowKey('ORDER_LINE', [w_id, d_id, no_o_id])
            rs = self._execute(q["sumOLAmount"],[rowkey])
            ol_total = 0
            for cell in rs.cells:
                if cell.key.column_family == 'OL_AMOUNT':
                    ol_total += float(cell.value)

            #self.cursor.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
            rowkey = self._getRowKey('NEW_ORDER',[w_id, d_id, no_o_id])
            self._execute(q["deleteNewOrder"], [rowkey])
            #self.cursor.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
            rowkey = self._getRowKey("ORDERS", [w_id, d_id, no_o_id])
            self._execute(q["updateOrders"], [rowkey, o_carrier_id])
            #self.cursor.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])
            rowkey = self._getRowKey('ORDER_LINE', [w_id, d_id, no_o_id])
            rs = self._execute(q['selectOrderLineKey'], [rowkey])
            for cell in rs.cells:
                rowkey = cell.key.row
                self._execute(q['updateOrderLine'], [rowkey, ol_delivery_d])

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            #assert ol_total > 0.0

            #self.cursor.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])
            rowkey = self._getRowKey('CUSTOMER', [w_id, d_id, c_id])
            rs = self._execute(q["getCustomerBalance"], [rowkey])
            for cell in rs.cells:
                if cell.key.column_family == 'C_BALANCE':
                    balance = float(cell.value)
                    break
            balance += ol_total
            self._execute(q["updateCustomerBalance"], [rowkey, balance])

            result.append((d_id, no_o_id))
        ## FOR

        ## Commit!

        return result


    def doNewOrder(self, params):
        startTime=time();
        htTime = 0
        q = TXN_QUERIES["NEW_ORDER"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            ## Determine if this is an all local order or not
            all_local = all_local and i_w_ids[i] == w_id
            rowkey = self._getRowKey('ITEM', [i_ids[i]])
            t=time()
            rs = self._execute(q["getItemInfo"], [rowkey])
            htTime+= time()-t
            itemInfo = {};
            for cell in rs.cells:
                itemInfo[cell.key.column_family] = cell.value
            items.append(itemInfo)
        assert len(items) == len(i_ids)

        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        for item in items:
            if len(item) == 0:
                ## TODO Abort here!
                return
        ## FOR

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        rowkey = self._getRowKey("WAREHOUSE", [w_id])
        t=time()
        rs = self._execute(q["getWarehouseTaxRate"], [rowkey])
        htTime+=time()-t
        w_tax = float(rs.cells[0].value)

        rowkey = self._getRowKey("DISTRICT", [w_id, d_id])
        t=time();
        rs = self._execute(q["getDistrict"], [rowkey])
        htTime+=time()-t;
        if(len(rs.cells) == 0):
            return
        [d_tax, d_next_o_id] = map(lambda cell: cell.value, rs.cells)
        d_tax = float(d_tax)

        rowkey = self._getRowKey("CUSTOMER", [w_id, d_id, c_id])
        t=time();
        rs = self._execute(q["getCustomer"], [rowkey])
        htTime+=time()-t;
        assert(len(rs.cells) > 0)
        customer_info = map(lambda cell : cell.value, rs.cells)[:3]
        c_discount = float(customer_info[2])
        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        t=time();

        rowkey = self._getRowKey("DISTRICT", [w_id, d_id])
        self._execute(q["incrementNextOrderId"], [rowkey,int(d_next_o_id) + 1])
        htTime+=time()-t;
        t=time();
        rowkey = self._getRowKey('ORDERS', [w_id, d_id, d_next_o_id])
        self._execute(q["createOrder"], [rowkey,d_next_o_id, rowkey,d_id, rowkey,w_id, rowkey, c_id, rowkey, o_entry_d, rowkey, o_carrier_id, rowkey, ol_cnt, rowkey, all_local])
        htTime+=time()-t;
        t=time();
        rowkey = self._getRowKey("NEW_ORDER", [w_id, d_id, d_next_o_id])
        self._execute(q["createNewOrder"], [rowkey, d_next_o_id, rowkey, d_id, rowkey, w_id])
        htTime+=time()-t;
        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]
            
            itemInfo = items[i]
            i_name = itemInfo['I_NAME']
            i_data = itemInfo['I_DATA']
            i_price = float(itemInfo['I_PRICE'])

            rowkey = self._getRowKey('STOCK', [ol_supply_w_id, ol_i_id])
            t=time();
            rs = self._execute(q["getStockInfo"] % (d_id), [rowkey])
            htTime+=time()-t;
            stockInfo = {}
            for cell in rs.cells:
                stockInfo[cell.key.column_family] = cell.value
            s_quantity = int(stockInfo['S_QUANTITY'])
            s_ytd = stockInfo['S_YTD']
            s_order_cnt = int(stockInfo['S_ORDER_CNT'])
            s_remote_cnt = int(stockInfo['S_REMOTE_CNT'])
            s_data = stockInfo['S_DATA']
            s_dist_xx = stockInfo['S_DIST_%02d' % d_id] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd = int(s_ytd) + ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = int(s_quantity) - ol_quantity
            else:
                s_quantity = int(s_quantity) + 91 - ol_quantity
            s_order_cnt += 1

            if ol_supply_w_id != w_id: s_remote_cnt += 1
            t=time();
            rowkey = self._getRowKey('STOCK', [ol_supply_w_id, ol_i_id])
            self._execute(q["updateStock"], [rowkey, s_quantity, rowkey, s_ytd, rowkey, s_order_cnt, rowkey, s_remote_cnt])
            htTime+=time()-t;
            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = float(ol_quantity) * i_price
            total += float(ol_amount)
            t=time();
            rowkey = self._getRowKey('ORDER_LINE',[w_id, d_id, d_next_o_id, ol_number])
            self._execute(q["createOrderLine"], [rowkey, d_next_o_id, rowkey, d_id, rowkey, w_id, rowkey, ol_number, rowkey, ol_i_id, rowkey, ol_supply_w_id, rowkey, o_entry_d, rowkey, ol_quantity, rowkey, ol_amount, rowkey, s_dist_xx])
            htTime+=time()-t;
            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR

        ## Commit!
        ## Do Nothing

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:",
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        print 'NewOrder ' + str(float(htTime)/(time() - startTime)) + ' Total time:  ' + str(time() - startTime)
        return [ customer_info, misc, item_data ]
        
    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            #getCustomerByCustomerId
            #self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            #customer = self.cursor.fetchone()
            rowkey = self._getRowKey('CUSTOMER',[w_id,d_id,c_id])

            rs = self._execute(q["getCustomerByCustomerId"],[rowkey])
            customer = map(lambda x: x.value, rs.cells)[:5]

        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            # rowkey = self._getRowKey('CUSTOMER', [w_id, d_id, '.'])
            # self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            # all_customers = self.cursor.fetchall()
            # assert len(all_customers) > 0
            # namecnt = len(all_customers)
            # index = (namecnt-1)/2
            # customer = all_customers[index]
            # c_id = customer[0]

            rowkey = self._getRowKey('CUSTOMER', [w_id, d_id])

            t = time()
            rs = self._execute(q['getCustomersWithoutCID'], [rowkey])
            print 'getCustomerWithoutCID : ' + str(time() - t) + " len: " + str(len(rs.cells))
            # dict (lastName, rowkey)
            dic = {}
            for cell in rs.cells:
                if cell.key.column_family == 'C_LAST':
                    dic[cell.value] = cell.key.row
            assert len(rs.cells) > 0
            rowkey = dic[dic.keys()[len(dic) >> 1]]
            rs = self._execute(q["getCustomerByCustomerId"],[rowkey])
            customer = map(lambda x: x.value, rs.cells)[:5]
            c_id = customer[0]

        assert len(customer) > 0
        assert c_id != None

        #self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
        #order = self.cursor.fetchone()
        rowkey = self._getRowKey('ORDERS', [w_id, d_id])
        rs = self._execute(q["getOrdersWithoutOID"], [rowkey])
        # get rowkeys which C_ID == c_id
        rows = map(lambda x: x.key.row, filter(lambda x: x.key.column_family == 'O_C_ID' and x.value == c_id, rs.cells))
        cells = filter(lambda x: x.key.row in rows, rs.cells)
        length = len(cells)
        if(length == 0):
            order = None
        else:
            assert length >= 3
            tuples = cells[length-3, length]
            assert tuples[0].key.row == tuples[1].key.row == tuples[2].key.row
            assert tuples[0].key.column_family == 'O_ID'
            assert tuples[1].key.column_family == 'O_CARRIER_ID'
            assert tuples[2].key.column_family == 'O_ENTRY_D'
            order = map(lambda x: x.value, tuples)

        if order:
            #self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
            #orderLines = self.cursor.fetchall()
            rowkey = self._getRowKey('ORDER_LINE', [w_id, d_id, order[0]])
            rs = self._execute(q["getOrderLines"], [rowkey])
            assert len(rs.cells) % 5 == 0
            orderLines = []
            for i in range(0, len(rs.cells) / 5):
                assert rs.cells[i].key.row == rs.cells[i+1].key.row == rs.cells[i+2].key.row == rs.cells[i+3].key.row == rs.cells[i+4].key.row
                assert rs.cells[i].key.column_family == 'OL_SUPPLY_W_ID'
                assert rs.cells[i+1].key.column_family == 'OL_I_ID'
                assert rs.cells[i+2].key.column_family == 'OL_QUANTITY'
                assert rs.cells[i+3].key.column_family == 'OL_AMOUNT'
                assert rs.cells[i+4].key.column_family == 'OL_DELIVERY_D'
                orderLines.append(map(lambda x: x.value, rs.cells[i:i+5]))
        else:
            orderLines = [ ]

        #Commit!
        #self.conn.commit()
        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        # if c_id != None:
        #     self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
        #     customer = self.cursor.fetchone()
        # else:
        #     # Get the midpoint customer's id
        #     self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
        #     all_customers = self.cursor.fetchall()
        #     assert len(all_customers) > 0
        #     namecnt = len(all_customers)
        #     index = (namecnt-1)/2
        #     customer = all_customers[index]
        #     c_id = customer[0]


        if c_id != None:
            rowkey = self._getRowKey('CUSTOMER',[w_id, d_id, c_id])
            rs = self._execute(q["getCustomerByCustomerId"],[rowkey])
            customer = map(lambda x: x.value, rs.cells)[:18]

        else:
            t = time()
            rowkey = self._getRowKey('CUSTOMER', [w_id, d_id])
            rs = self._execute(q['getCustomersWithoutCID'], [rowkey])
            print 'getCustomerWithoutCID : ' + str(time() - t) + " len: " + str(len(rs.cells))
            # dict (lastName, rowkey)
            dic = {}
            for cell in rs.cells:
                if cell.key.column_family == 'C_LAST':
                    dic[cell.value] = cell.key.row
            # Get the midpoint customer's rowkey
            rowkey = dic[dic.keys()[len(dic) >> 1]]
            rs = self._execute(q["getCustomerByCustomerId"],[rowkey])
            customer = map(lambda x: x.value, rs.cells)[:18]
            c_id = customer[0]

        assert customer is not None
        assert len(customer) > 0
        c_balance = float(customer[14]) - h_amount
        c_ytd_payment = float(customer[15]) + h_amount
        c_payment_cnt = float(customer[16]) + 1
        c_data = customer[17]

        #self.cursor.execute(q["getWarehouse"], [w_id])
        #warehouse = self.cursor.fetchone()
        rowkey = self._getRowKey('WAREHOUSE', [w_id])
        rs = self._execute(q["getWarehouse"], [rowkey])
        warehouse = map(lambda x: x.value, rs.cells)[:6]
        warehouseBalance = rs.cells[6].value

        #self.cursor.execute(q["getDistrict"], [w_id, d_id])
        #district = self.cursor.fetchone()
        rowkey = self._getRowKey('DISTRICT', [w_id, d_id])
        rs = self._execute(q["getDistrict"], [rowkey])
        district = map(lambda x: x.value, rs.cells)[:6]
        districtBalance = rs.cells[6].value

        #self.cursor.execute(q["updateWarehouseBalance"], [h_amount, w_id])
        #self.cursor.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])
        rowkey = self._getRowKey('WAREHOUSE', [w_id])
        self._execute(q["updateWarehouseBalance"], [rowkey, float(warehouseBalance) + h_amount])

        rowkey = self._getRowKey('DISTRICT', [w_id, d_id])
        rs = self._execute(q["updateDistrictBalance"], [rowkey, float(districtBalance) + h_amount])

        # Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            #self.cursor.execute(q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
            rowkey = self._getRowKey('CUSTOMER', [c_w_id, c_d_id, c_id])
            self._execute(q["updateBCCustomer"], [rowkey, c_balance, rowkey, c_ytd_payment, rowkey, c_payment_cnt, rowkey, c_data])
        else:
            c_data = ""
            #self.cursor.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])
            rowkey = self._getRowKey('CUSTOMER', [c_w_id, c_d_id, c_id])
            self._execute(q["updateGCCustomer"], [rowkey, c_balance, rowkey, c_ytd_payment, rowkey, c_payment_cnt])

        # Concatenate w_name, four spaces, d_name
        h_data = "%s %s" % (warehouse[0], district[0])
        # Create the history record
        #self.cursor.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])
        rowkey = self._getRowKey('HISTORY', [c_id])
        self._execute(q["insertHistory"], [rowkey, c_id, rowkey, c_d_id, rowkey, c_w_id, rowkey, d_id, rowkey, w_id, rowkey, h_date, rowkey, h_amount, rowkey, h_data])

        #Commit!
        #self.conn.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]

    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        #Self.Cursor.Execute(Q["getOId"], [w_id, d_id])
        #result = self.cursor.fetchone()
        #assert result
        #o_id = result[0]
        rowkey = self._getRowKey('DISTRICT', [w_id, d_id])
        rs = self._execute(q["getOId"], [rowkey])
        o_id = rs.cells[0].value

        #self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
        #result = self.cursor.fetchone()

        rowkey = self._getRowKey('ORDER_LINE', [w_id, d_id])
        t = time()
        rs = self._execute(q['getOrderLineInfo'], [rowkey])
        print 'getOrderLineInfo : ' + str(time() - t) + " len: " + str(len(rs.cells))
        rowkeys = map(lambda cell: cell.key.row, filter(lambda cell: cell.key.column_family == 'OL_O_ID' and int(o_id) - 20 <= int(cell.value) < int(o_id), rs.cells))
        orderLineIIDs = map(lambda cell: cell.value, filter(lambda cell: cell.key.row in rowkeys and cell.key.column_family == 'OL_I_ID', rs.cells))

        rowkey = self._getRowKey('STOCK', [w_id])
        rs = self._execute(q['getStockInfo'], [rowkey])
        rowkeys = map(lambda cell: cell.key.row, filter(lambda cell: cell.key.column_family == 'S_QUANTITY' and cell.value < threshold, rs.cells))
        stockIIDs = map(lambda cell: cell.value, filter(lambda cell:cell.key.row in rowkeys and cell.key.column_family == 'S_I_ID', rs.cells))

        orderLineIIDs.sort()
        stockIIDs.sort()

        count = 0
        oi = 0
        si = 0
        while(oi < len(orderLineIIDs) and si < len(stockIIDs)):
            if orderLineIIDs[oi] < stockIIDs[si] :
                oi += 1
            elif orderLineIIDs[oi] > stockIIDs[si] :
                si += 1
            else:
                oi += 1
                si += 1
                count += 1

        #Commit!
        #self.conn.commit()

        return int(count)
