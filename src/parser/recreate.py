#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time
import re
import argparse
import yaml
import json
import logging
from pymongo import Connection

sys.path.append("../workload")
from traces import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

### DEFAULT VALUES
### you can specify these with args
WORKLOAD_DB = "metadata"
WORKLOAD_COLLECTION = "workload01"
RECREATED_DB = "dataset"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

###GLOBAL VARS
connection = None
current_transaction = None

workload_db = None
workload_col = None
recreated_db = None
recreated_col = None


#returns collection where the traces (Session objects) are stored
def getWorkloadCol():
    return workload_db[workload_col]


def initDB(hostname, port, w_db, w_col, r_db):
    global connection
    global workload_db
    global recreated_db
    global workload_col

    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    workload_db = connection[w_db]
    workload_col = str(w_col)
    recreated_db = connection[r_db]
    
    return

def cleanRecreated():
    recreated_db.command("dropDatabase")

#
# Handling individual types of operations
#
def processInsert(op):
    payload = op["query_content"]
    col = op["collection"]
    LOG.info("Inserting %d documents into collection %s", len(payload), col)
    for doc in payload:
        print "inserting: ", doc
        recreated_db[col].save(doc)

def processDelete(op):
    payload = op["query_content"]
    col = op["collection"]
    #for doc in payload:
    LOG.info("Deleting documents from collection %s..", col)
    recreated_db[col].remove(payload)

def processUpdate(op):
    payload = op["query_content"]
    col = op["collection"]
    upsert = op["update_upsert"]
    multi = op["update_multi"]
    #for doc in payload:
    LOG.info("Updating collection %s. Upsert: %s, Multi: %s", col,str(upsert), str(multi))
    if len(payload) != 2:
        LOG.warn("Update operation payload is expected to have exactly 2 entries.")
    else:
        recreated_db[col].update(payload[0], payload[1], upsert, multi)

def processQuery(op):
    if op["query_aggregate"] == 1:
        # This is probably AGGREGATE... disregard it
        return
    
    # check if resp_content was set
    if 'resp_content' not in op:
        LOG.warn("Query without response: %s" % str(op))
        return
    
    # The query is irrelevant, we simply add the content of the reply...
    payload = op["resp_content"]
    col = op["collection"]
    LOG.info("Adding %d query results to collection %s", len(payload), col)
    #update(old_doc, new_doc, upsert=True, multi=False)
    #this is an upsert operation: insert if not present
    for doc in payload:
        #print "doc:", doc
        recreated_db[col].update(doc, doc, True, False)
    

#
# Iterates through all operations of all sessions
# and recreates the dataset...
#
def processTraces():
    cnt = getWorkloadCol().find().count()
    LOG.info("Found %d sessions in the workload collection. Processing... ", cnt)
    for session in getWorkloadCol().find():
        for op in session["operations"]:
            if (op["type"] == "$insert"):
                processInsert(op)
            elif (op["type"] == "$delete"):
                processDelete(op)
            elif (op["type"] == "$query"):
                processQuery(op)
            elif (op["type"] == "$update"):
                processUpdate(op)
            elif (op["type"] == "$isert"):
                processInsert(op)
            else:
                LOG.warn("Unknow operation type: %s", op["type"])
    LOG.info("Done.")

def main():    
    aparser = argparse.ArgumentParser(description='MongoDesigner Datase Recreator')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--workload_db', help='the database where you want to store the traces', default=WORKLOAD_DB)
    aparser.add_argument('--workload_col', help='the collection where you want to store the traces', default=WORKLOAD_COLLECTION)
    
    aparser.add_argument('--recreated_db', help='the database of the recreated dataset', default=RECREATED_DB)
    
    aparser.add_argument('--clean', action='store_true',
                         help='Remove all documents in the recreated collection before processing is started')
    args = vars(aparser.parse_args())
    
    LOG.info("\n..:: MongoDesigner Dataset Recreator ::..\n")
    
    #Start DB connection and initialize global vars...
    initDB(args['host'], args['port'], args['workload_db'], args['workload_col'], args['recreated_db'])
    
    settings = "host: ", args['host'], " port: ", args['port'], " workload_db: ", args['workload_db'], " workload_col: ", args['workload_col'], " recreated_db: ", args['recreated_db'] 
    LOG.info("Settings: %s", settings)

    if args['clean']:
        LOG.warn("Cleaning '%s' collection...", recreated_db)
        cleanRecreated()
   
    processTraces()
    
    return
    
if __name__ == '__main__':
    main()



    
