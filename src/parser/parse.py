#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011 by Brown University
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
from __future__ import with_statement

import os
import sys
import fileinput
import hashlib
import time
import re
import argparse
import yaml
import json
import logging
from pprint import pformat
from pymongo import Connection

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# MongoDB-Designer
sys.path.append(os.path.join(basedir, "../workload"))
sys.path.append(os.path.join(basedir, "../sanitizer"))
import workload_info
from traces import *
import anonymize # just for hash_string()

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

## ==============================================
## DEFAULT VALUES
## you can specify these with args
## ==============================================
INPUT_FILE = "sample.txt"
WORKLOAD_DB = "metadata"
WORKLOAD_COLLECTION = "workload01"
INITIAL_SESSION_UID = 100 #where to start the incremental session uid
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

## ==============================================
## GLOBAL VARS
## ==============================================
connection = None
current_transaction = None
workload_db = None
workload_col = None
recreated_db = None

# current session map holds all session objects. Mapping client_id --> Session()
current_session_map = {} 
session_uid = INITIAL_SESSION_UID # first session_id

# used to pair up queries & replies by their mongosniff ID
query_response_map = {} 

# Post-processing global vars. PLAINTEXT Collection Names for AGGREGATES
# this dictionary is used to figure out the real collection names for aggregate queries
# the col names are hashed
# STEP1: during the first pass (the main step of parsing), we store the names of all collections
# we encounter in the set() known_collections
# STEP2: we figure out the salt
# STEP3: we compute the hash. We populate the dict  hashed_collections
# STEP4: we add the collection names to all aggregate operations
known_collections = set() # set of known collection names
hashed_collections = {} # hash --> collection name

### parsing regexp masks
### parts of header
TIME_MASK = "[0-9]+\.[0-9]+.*"
ARROW_MASK = "(-->>|<<--)"
IP_MASK = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{5,5}"
COLLECTION_MASK = "[\w+\.]+\$?\w+"
SIZE_MASK = "\d+ bytes"
MAGIC_ID_MASK = "id:\w+"
TRANSACTION_ID_MASK = "\d+"
REPLY_ID_MASK = "\d+"

## ==============================================
## PARSING REGEXES
## ==============================================

### header
HEADER_MASK = "(?P<timestamp>" + TIME_MASK + ") *- *" + \
    "(?P<IP1>" + IP_MASK + ") *" + \
    "(?P<arrow>" + ARROW_MASK + ") *" + \
    "(?P<IP2>" + IP_MASK + ") *" + \
    "(?P<collection>" + COLLECTION_MASK + ")? *" + \
    "(?P<size>" + SIZE_MASK + ") *" + \
    "(?P<magic_id>" + MAGIC_ID_MASK + ")[\t ]*" + \
    "(?P<trans_id>" + TRANSACTION_ID_MASK + ")[\t ]*" + \
    "-?[\t ]*(?P<query_id>" + REPLY_ID_MASK + ")?"
headerRegex = re.compile(HEADER_MASK)

### content lines
CONTENT_REPLY_MASK = "\s*reply +.*"
CONTENT_INSERT_MASK = "\s*insert: {.*"
CONTENT_QUERY_MASK = "\s*query: {.*"
CONTENT_UPDATE_MASK = "\s*update .*"
CONTENT_DELETE_MASK = "\s*delete .*"

replyRegex = re.compile(CONTENT_REPLY_MASK)
insertRegex = re.compile(CONTENT_INSERT_MASK)
queryRegex = re.compile(CONTENT_QUERY_MASK)
updateRegex = re.compile(CONTENT_UPDATE_MASK)
deleteRegex = re.compile(CONTENT_DELETE_MASK)

# other masks for parsing
FLAGS_MASK = ".*flags:(?P<flags>\d).*" #vals: 0,1,2,3
flagsRegex = re.compile(FLAGS_MASK)
NTORETURN_MASK = ".*ntoreturn: (?P<ntoreturn>-?\d+).*" # int 
ntoreturnRegex = re.compile(NTORETURN_MASK)
NTOSKIP_MASK = ".*ntoskip: (?P<ntoskip>\d+).*" #int
ntoskipRegex = re.compile(NTOSKIP_MASK)

# op TYPES
TYPE_QUERY = '$query'
TYPE_INSERT = '$insert'
TYPE_DELETE = '$delete'
TYPE_UPDATE = '$update'
TYPE_REPLY = '$reply'
QUERY_TYPES = [TYPE_QUERY, TYPE_INSERT, TYPE_DELETE, TYPE_UPDATE]

## ==============================================

#returns collection where the traces (Session objects) are stored
def getTracesCollection():
    return workload_db[workload_col]

def initDB(hostname, port, w_db, w_col):
    global connection
    global workload_db
    global workload_col

    LOG.info("Connecting to MongoDB at %s:%d" % (hostname, port))
    
    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    workload_db = connection[w_db]
    workload_col = w_col

    return

def cleanWorkload():
    getTracesCollection().remove()


# helper method to split IP and port
def getOnlyIP(ipAndPort):
    l = ipAndPort.rsplit(":") # we can be sure that ipAndPort is in the form of IP:port since it was matched by regex...
    return l[0]

#
# this function initializes a new Session() object (in workload/traces.py)
# and sotres it in the collection
# ip1 is the key in current_transaction_map
#
def addSession(ip_client, ip_server):
    global current_session_map
    global session_uid
    global workload_db
        
    #verify a session with the uid does not exist
    if getTracesCollection().find({'session_id': session_uid}).count() > 0:
        msg = "Session with UID %s already exists.\n" % session_uid
        msg += "Maybe you want to clean the database / use a different collection?"
        raise Exception(msg)

    session = Session()
    session['ip_client'] = unicode(ip_client)
    session['ip_server'] = unicode(ip_server)
    session['session_id'] = session_uid
    current_session_map[ip_client] = session
    session_uid = session_uid + 1
    getTracesCollection().save(session)
    return session

def store(transaction):
    
    if (current_transaction['arrow'] == '-->>'):
        ip_client = current_transaction['IP1']
        ip_server = current_transaction['IP2']
    else:
        ip_client = current_transaction['IP2']
        ip_server = current_transaction['IP1']

    if (ip_client not in current_session_map):
        session = addSession(ip_client, ip_server)
    else:
        session = current_session_map[ip_client]
    
    if 'type' not in current_transaction:
        LOG.error("INCOMPLETE operation:")
        LOG.error(current_transaction)
        return
    
    # QUERY: $query, $delete, $insert, $update:
    # Create the operation, add it to the session
    if current_transaction['type'] in QUERY_TYPES:
        # create the operation -- corresponds to current_transaction
        query_id = current_transaction['trans_id'];
        op = {
                'collection': unicode(current_transaction['collection']),
                'type': unicode(current_transaction['type']),
                'query_time': float(current_transaction['timestamp']),
                'query_size': int(current_transaction['size'].replace("bytes", "")),
                'query_content': current_transaction['content'],
                'query_id': int(query_id),
                'query_aggregate': 0, # false -not aggregate- by default
        }
        
        # UPDATE flags
        if op['type'] == TYPE_UPDATE:
            op['update_upsert'] = current_transaction['update_upsert']
            op['update_multi'] = current_transaction['update_multi']
        
        # QUERY 
        if op['type'] == TYPE_QUERY:
            # SKIP, LIMIT
            op['query_limit'] = int(current_transaction['ntoreturn']['ntoreturn'])
            op['query_offset'] = int(current_transaction['ntoskip']['ntoskip'])
        
            # check for aggregate
            # update collection name, set aggregate type
            if op['collection'].find("$cmd") > 0:
                op['query_aggregate'] = 1
                # extract the real collection name
                ## --> This has to be done at the end after the first pass, because the collection name is hashed up
        
        query_response_map[query_id] = op
        # append it to the current session
        session['operations'].append(op)
        LOG.debug("added operation: %s" % op)
    
        # store the collection name in known_collections. This will be useful later.
        # see the comment at known_collections
        global known_collections
        full_name = op['collection']
        col_name = full_name[full_name.find(".")+1:] #cut off the db name
        known_collections.add(col_name)
    
    # RESPONSE - add information to the matching query
    if current_transaction['type'] == "$reply":
        query_id = current_transaction['query_id'];
        # see if the matching query is in the map
        if query_id in query_response_map:
            # fill in missing information
            query_op = query_response_map[query_id]
            query_op['resp_content'] = current_transaction['content']
            query_op['resp_size'] = int(current_transaction['size'].replace("bytes", ""))
            query_op['resp_time'] = float(current_transaction['timestamp'])
            query_op['resp_id'] = int(current_transaction['trans_id'])    
        else:
            LOG.warn("SKIPPING RESPONSE (no matching query_id): %s" % query_id)
            
    #save the current session
    getTracesCollection().save(session)
    
    LOG.debug("session %d was updated" % session['session_id'])
    return


def process_header_line(header):
    global current_transaction

    if current_transaction:
        try:
            store(current_transaction)
        except:
            LOG.error("Invalid Session:\n%s" % pformat(current_transaction))
            raise

    current_transaction = header
    current_transaction['content'] = []
    
    return


# helper function for process_content_line 
# takes yaml {...} as input
# parses the input to JSON and adds that to current_transaction['content']
def add_yaml_to_content(yaml_line):
    global current_transaction
    
    yaml_line = yaml_line.strip()
    
    #skip empty lines
    if len(yaml_line) == 0:
        return

    if not yaml_line.startswith("{"):
        # this is not a content line... it can't be yaml
        LOG.warn("JSON does not start with '{'")
        LOG.debug("Offending Line: %s" % yaml_line)
        return
    
    if not yaml_line.strip().endswith("}"):
        LOG.warn("JSON does not end with '}'")
        LOG.debug(yaml_line)
        return    
    
    #yaml parser might fail :D
    try:
        obj = yaml.load(yaml_line)
    except (yaml.scanner.ScannerError, yaml.parser.ParserError, yaml.reader.ReaderError) as err:
        LOG.error("Parsing yaml to JSON: " + str(yaml_line))
        LOG.error("details: " + str(err))
        #print yaml_line
        #exit()
        raise
    
    valid_json = json.dumps(obj)
    obj = yaml.load(valid_json)
    if not obj:
        LOG.error("Weird error. This line parsed to yaml, not to JSON: " + str(yaml_line))
        return 
    
    #if this is the first time we see this session, add it
    if 'whatismyuri' in obj:
        addSession(current_transaction['ip_client'], current_transaction['ip_server'])
    
    #store the line
    current_transaction['content'].append(obj)
    return

# takes any line which does not pass as header line
# tries to figure out the transaction type & store the content
def process_content_line(line):
    global current_transaction
    
    # ignore content lines before the first transaction is started
    if (not current_transaction):
        return

    # REPLY
    if (replyRegex.match(line)):
        current_transaction['type'] = TYPE_REPLY
    
    #INSERT
    elif (insertRegex.match(line)):
        current_transaction['type'] = TYPE_INSERT
        line = line[line.find('{'):line.rfind('}')+1]
        add_yaml_to_content(line)
    
    # QUERY
    elif (queryRegex.match(line)):
        current_transaction['type'] = TYPE_QUERY
        
        # extract OFFSET and LIMIT
        current_transaction['ntoskip'] = ntoskipRegex.match(line).groupdict()
        current_transaction['ntoreturn'] = ntoreturnRegex.match(line).groupdict()
        
        line = line[line.find('{'):line.rfind('}')+1]
        add_yaml_to_content(line)
        
    # UPDATE
    elif (updateRegex.match(line)):
        current_transaction['type'] = TYPE_UPDATE
        
        # extract FLAGS
        upsert=False
        multi=False
        flags = flagsRegex.match(line).groupdict()
        if flags=='1':
            upsert=True
            multi=False
        if flags=='2':
            upsert=False
            multi=True
        if flags=='3':
            upsert=True
            multi=True
        current_transaction['update_upsert']=upsert
        current_transaction['update_multi']=multi
        
        # extract the CRITERIA and NEW_OBJ
        lines = line[line.find('{'):line.rfind('}')+1].split(" o:")
        if len(lines) > 2:
            LOG.error("Fuck. This update query is tricky to parse: " + str(line))
            LOG.error("Skipping it for now...")
        if len(lines) < 2:
            return
        add_yaml_to_content(lines[0])
        add_yaml_to_content(lines[1])
    
    # DELETE
    elif (deleteRegex.match(line)):
        current_transaction['type'] = TYPE_DELETE
        line = line[line.find('{'):line.rfind('}')+1] 
        add_yaml_to_content(line) 
    
    # GENERIC CONTENT LINE
    else:
        #default: probably just yaml content line...
        add_yaml_to_content(line) 
    return


'''
Post-processing: infer plaintext collection names for AGGREGATES
'''
# this functions returns a set of some hashed strings, which are most likely hashed collection names
def get_candidate_hashes():
    candidate_hashes = set()
    LOG.info("Retrieving hashed collection names...")
    for session in getTracesCollection().find():
        for op in session['operations']:
            if op['query_aggregate'] == 1:
                # find the JSON of the query...
                query = op['query_content'][0] # we care about the first (0th) BSON in the list
                # look four count key. This would refer to a collection name
                if 'count' in query:
                    #print query
                    candidate_hashes.add(query['count'])
    LOG.info("Found %d hashed collection names. " % len(candidate_hashes))
    print(candidate_hashes)
    return candidate_hashes

def get_hash_string(bare_col_name):
    return "\"" + bare_col_name + "\""

# this is a ridiculous hack. Let's hope the salt is 0. But even if not...
def infer_salt(candidate_hashes, known_collections):
    max_salt = 100000
    LOG.info("Trying to brute-force the salt 0-%d..." % max_salt)
    salt = 0
    while True:
        if salt % (max_salt / 100) == 0:
            print ".",
        for known_col in known_collections:
            hashed_string = get_hash_string(known_col) # the col names are hashed with quotes around them 
            hash = anonymize.hash_string(hashed_string, salt) # imported from anonymize.py
            if hash in candidate_hashes:
                LOG.info("SUCCESS! %s hashes to a known value. SALT: %d", hashed_string, salt)
                return salt
        salt += 1
        if salt > max_salt:
            break
    LOG.info("FAIL. The salt value is unknown :(")
    return None


# this function populates the hashed_collections map
# mapping HASHED_COL_NAME -> PLAIN_TEXT_COL_NAME
def precompute_hashes(salt):
    LOG.info("Precomputing hashes for all known collection names...")
    global hashed_collections
    for col_name in known_collections:
        hash = anonymize.hash_string(get_hash_string(col_name), salt)
        hashed_collections[hash] = col_name
        print "hash: ", hash, "col_name: ", col_name, " hash_str: ", get_hash_string(col_name)
    LOG.info("Done.")

# now we go through aggregate ops again and fill in the collection name...
def fill_aggregate_collection_names():
    LOG.info("Adding plaintext collection names to aggregate operations...")
    cnt = 0
    for session in getTracesCollection().find():
        for op in session['operations']:
            if op['query_aggregate'] == 1:
                query = op['query_content'][0] # first and only JSON from the payload
                # iterate through the keys in the query JSON
                # one of the should point to the hashed collection name
                for key in query:
                    value = query[key]
                    #print "value: ", value, " type: ", type(value)
                    if type(value) is type(u''):
                        #print "candidate val: ", value
                        if value in hashed_collections:
                            # YES. We found it!
                            # contains $cmd. Just to double-check
                            if op['collection'].find("$cmd") < 0:
                                LOG.warn("Aggregate operation does not seem to be aggregate. Skipping.")
                                print op
                                continue
                            col_name = hashed_collections[value] # the plaintext collection name is restored
                            db_name = op['collection'].split(".")[0] #extract the db name from db.$cmd
                            cnt += 1
                            op['collection'] = db_name + "." + col_name
                        ### if
                    ### if
                ### for        
            ### if
        ### for
        # save the session
        getTracesCollection().save(session)
    ### for
        
    LOG.info("Done. Updated %d aggregate operations." % cnt)

# CALL THIS FUNCTION TO DO THE POST-PROCESSING
def infer_aggregate_collections():
    LOG.info("")
    LOG.info("-- Aggregate Collection Names --")
    LOG.info("Encountered %d collection names in plaintext." % len(known_collections))
    print(known_collections)
    candidate_hashes = get_candidate_hashes()
    salt = infer_salt(candidate_hashes, known_collections)
    if salt is None:
        return
    precompute_hashes(salt)
    fill_aggregate_collection_names()

'''
END OF Post-processing: AGGREGATE collection names
'''


def parseFile(file):
    LOG.info("Processing file %s", file)
    line_ctr = 0
    trans_ctr = 0
    with open(file, 'r') as fd:
        for line in fd:
            line_ctr += 1
            result = headerRegex.match(line)
            #print line
            try:
                if result:
                    process_header_line(result.groupdict())
                    trans_ctr += 1
                else:
                    process_content_line(line)
            except:
                LOG.error("Unexpected error when processing line %d" % line_ctr)
                raise
        ## FOR
    if current_transaction:
        store(current_transaction)

    print ""
    session_ctr = INITIAL_SESSION_UID - session_uid
    LOG.info("Done. Added [%d traces], [%d sessions] to '%s'" % (trans_ctr, session_ctr, workload_col))
        

# STATS - print out some information when parsing finishes
def print_stats(args):
    workload_info.print_stats(args['host'], args['port'], args['workload_db'], args['workload_col'])

if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='MongoDesigner Trace Parser')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--file',
                         help='file to read from', default=INPUT_FILE)
    aparser.add_argument('--workload_db', help='the database where you want to store the traces', default=WORKLOAD_DB)
    aparser.add_argument('--workload_col', help='the collection where you want to store the traces', default=WORKLOAD_COLLECTION)
    aparser.add_argument('--clean', action='store_true',
                         help='Remove all documents in the workload collection before processing is started')    
    args = vars(aparser.parse_args())

    print ""
    LOG.info("..:: MongoDesigner Trace Parser ::..")
    print ""

    settings = "host: ", args['host'], " port: ", args['port'], " file: ", args['file'], " db: ", args['workload_db'], " col: ", args['workload_col']
    LOG.info("Settings: %s", settings)

    # initialize connection to MongoDB
    initDB(args['host'], args['port'], args['workload_db'], args['workload_col'])

    # wipe the collection
    if args['clean']:
        LOG.warn("Cleaning '%s' collection...", workload_col)
        cleanWorkload()
    
    # parse
    parseFile(args['file'])
    
    # Post-processing: fill in aggregate collection names in plaintext
    infer_aggregate_collections()
    
    # print info
    print_stats(args)
    
## MAIN


    
