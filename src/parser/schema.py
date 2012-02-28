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

sys.path.append("../")
from catalog import Collection

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

### DEFAULT VALUES
### you can specify these with args
RECREATED_DB = "recreated"
SCHEMA_DB = "schema"
SCHEMA_COL = "schema"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

###GLOBAL VARS
connection = None
current_transaction = None

recreated_db = None
schema_db = None
schema_col = None





def initDB(hostname, port, r_db, s_db, s_col):
    global connection
    global recreated_db
    global schema_db
    global schema_col

    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    recreated_db = connection[r_db]
    schema_db = connection[s_db]
    schema_col = s_col
    
    return

def cleanSchema():
    schema_db.command("dropDatabase")

#nested is True/False flag
def addKeys(fields, doc, nested):
    for k in doc.keys():
        fields[k]={}
        if type(doc[k]) is type({}):
            addKeys(fields, doc[k], True)

#
# Iterates through all documents and infers the schema...
#
def processTraces():
    cols = recreated_db.collection_names()
    LOG.info("Found %d collections. Processing...", len(cols))
    for col in cols:
        if col.startswith("system."):
            continue
        c = Collection()
        c['name'] = col
        fields = {}
        for doc in recreated_db[col].find():
            addKeys(fields, doc, False)
        c['fields'] = fields
        schema_db[schema_col].insert(c)
        
def cleanSchema():
    schema_db.command("dropDatabase")

def main():    
    aparser = argparse.ArgumentParser(description='MongoDesigner Datase Recreator')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--recreated_db', help='the database containg the recreated dataset', default=RECREATED_DB)
    
    aparser.add_argument('--schema_db', help='the database of the schema catalog', default=SCHEMA_DB)
    aparser.add_argument('--schema_col', help='the collection of the schema catalog', default=SCHEMA_COL)
    
    aparser.add_argument('--clean', action='store_true',
                         help='Remove all documents in the recreated collection before processing is started')
    args = vars(aparser.parse_args())
    
    LOG.info("\n..:: MongoDesigner Schema Catalog Recreator ::..\n")
    
    #Start DB connection and initialize global vars...
    initDB(args['host'], args['port'], args['recreated_db'], args['schema_db'], args['schema_col'])
    
    settings = "host: ", args['host'], " port: ", args['port'], " recreated_db: ", args['recreated_db'], " schema_db: ", args['schema_db'], " schema_col: ", args['schema_col']
    LOG.info("Settings: %s", settings)

    if args['clean']:
        LOG.warn("Cleaning '%s' db...", schema_db)
        cleanSchema()
   
    processTraces()
    
    return
    
if __name__ == '__main__':
    main()



    
