#!/usr/bin/env python
from pymongo import Connection
import sys
import logging
sys.path.append("../workload")
from traces import *
import argparse
logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

### DEFAULT VALUES
WORKLOAD_DB = "metadata"
WORKLOAD_COLLECTION = "workload01"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

# global vars
workload_db = None
workload_col = None

def initDB(hostname, port, w_db, w_col):
    global workload_db
    global workload_col
    LOG.info("Connecting to MongoDB at %s:%d" % (hostname, port))
    
    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    workload_db = connection[w_db]
    workload_col = w_col

    return connection


def print_stats(host, port, w_db, w_col):
    print ""
    LOG.info("..:: MongoDesigner Workload Info ::..")
    print ""

    #start connection and set global variables...
    connection = initDB(host, port, w_db, w_col)

    
    LOG.info("")
    LOG.info("========================")
    
    session_cnt = workload_db[workload_col].find().count()
    LOG.info("Number of sessions: %d", session_cnt)
    LOG.info("Number of operations per session:")
    
    max=0
    min=sys.maxint
    vals=[]
    for session in workload_db[workload_col].find():
        op_cnt = len(session['operations'])
        if op_cnt > max:
            max = op_cnt
        if op_cnt < min:
            min = op_cnt
        vals.append(op_cnt)
    avg = float(sum(vals)) / float(len(vals))
    LOG.info("      min: %d", min)
    LOG.info("      max: %d", max)
    LOG.info("      avg: %d", avg)
    
    LOG.info("Number of operations by type:")
    delete_cnt=0
    update_cnt=0
    insert_cnt=0
    query_cnt=0
    for session in workload_db[workload_col].find():
        for op in session['operations']:
            if op['type']=="$update":
                update_cnt+=1
            if op['type']=="$insert":
                insert_cnt+=1
            if op['type']=="$delete":
                delete_cnt+=1
            if op['type']=="$query":
                query_cnt+=1
    LOG.info("      query: %d", query_cnt)
    LOG.info("      insert: %d", insert_cnt)
    LOG.info("      delete: %d", delete_cnt)
    LOG.info("      update: %d", update_cnt)
    
    LOG.info("========================")
    
    
    return


def main():
    global headerRegex

    aparser = argparse.ArgumentParser(description='MongoDesigner Workload Info')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--workload_db', help='the database where you want to store the traces', default=WORKLOAD_DB)
    aparser.add_argument('--workload_col', help='the collection where you want to store the traces', default=WORKLOAD_COLLECTION)
    args = vars(aparser.parse_args())

    
    print_stats(args['host'], args['port'], args['workload_db'], args['workload_col'])

if __name__ == '__main__':
        main()



    
