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
INPUT_FILE = "sample.txt"
WORKLOAD_DB = "metadata"
WORKLOAD_COLLECTION = "sessions"
INITIAL_SESSION_UID = 100 #where to start the incremental session uid
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

###GLOBAL VARS
connection = None
current_transaction = None
workload_db = None
workload_col = None
recreated_db = None

current_session_map = {}
session_uid = INITIAL_SESSION_UID

query_response_map = {}

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
### header
HEADER_MASK = "(?P<timestamp>" + TIME_MASK + ") *- *" + \
"(?P<IP1>" + IP_MASK + ") *" + \
"(?P<arrow>" + ARROW_MASK + ") *" + \
"(?P<IP2>" + IP_MASK + ") *" + \
"(?P<collection>" + COLLECTION_MASK + ")? *" + \
"(?P<size>" + SIZE_MASK + ") *" + \
"(?P<magic_id>" + MAGIC_ID_MASK + ") *" + \
"(?P<transaction_id>" + TRANSACTION_ID_MASK + ") *" + \
"-? *(?P<query_id>" + REPLY_ID_MASK + ")?"
headerRegex = re.compile(HEADER_MASK);
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

#returns collection where the traces (Session objects) are stored
def getTracesCollection():
    return workload_db[workload_col]

def initDB(hostname, port, w_db, w_col):
    global connection
    global workload_db
    global workload_col

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
def addSession(ip1, ip2):
    global current_session_map
    global session_uid
    global workload_db
        
        #verify a session with the uid does not exist
    if getTracesCollection().find({'uid': session_uid}).count() > 0:
        LOG.error("Error: Session with UID %s already exists." % session_uid)
        LOG.error("Maybe you want to clean the database / use a different collection?")
        sys.exit(0)

    session = Session()
    session['ip1'] = unicode(ip1)
    session['ip2'] = unicode(ip2)
    session['uid'] = session_uid
    current_session_map[current_transaction['IP1']] = session
    session_uid = session_uid + 1
    getTracesCollection().save(session)
    return session

def store(transaction):
    #print ""
    #print ""
    #print current_transaction
    if (current_transaction['arrow'] == '-->>'):
        ip1 = current_transaction['IP1']
        ip2 = current_transaction['IP2']
    else:
        ip1 = current_transaction['IP2']
        ip2 = current_transaction['IP1']

    if (ip1 not in current_session_map):
        session = addSession(ip1, ip2)
    else:
        session = current_session_map[ip1]
    
    if 'type' not in current_transaction:
        LOG.error("INCOMPLETE operation:")
        LOG.error(current_transaction)
        return
    
    # create the operation -- corresponds to current_transaction
    op = {
            'collection': unicode(current_transaction['collection']),
            'content': current_transaction['content'],
            'timestamp': float(current_transaction['timestamp']),
            'type': unicode(current_transaction['type']),
            'size': int(current_transaction['size'].replace("bytes", "")),
    }
    
    # QUERY - add entry to the query-response map so we can add its response in the future
    if current_transaction['type'] == "$query":
        query_id = current_transaction['transaction_id'];
        op['response'] = None # TO BE FILLED later
        query_response_map[query_id] = op
    
    # RESPONSE - append it to the query
    if current_transaction['type'] == "$reply":
        query_id = current_transaction['query_id'];
        if query_id in query_response_map:
            query = query_response_map[query_id]
            query['response'] = op
        else:
            print "SKIPPED QUERY"
            print op
    else:
        # Append the operation to the current session
        session['operations'].append(op)
        LOG.debug("inserting operation: %s" % op)
    
    #save the session
    getTracesCollection().save(session)
    
    LOG.debug("session %d was updated" % session['uid'])
    return


def process_header_line(header):
    global current_transaction

    if (current_transaction):
        store(current_transaction)

    current_transaction = header
    current_transaction['content'] = []
    
    return


# helper function for process_content_line 
# takes yaml {...} as input
# parses the input to JSON and adds that to current_transaction['content']
def add_yaml_to_content(yaml_line):
    global current_transaction
    
    #skip empty lines
    if len(yaml_line.split()) is 0:
        return
    
    if not yaml_line.strip().startswith("{"):
        # this is not a content line... it can't be yaml
        print "SKIPPING:"
        print yaml_line
        return
    
    if yaml_line.strip().startswith("{"):
        if not yaml_line.strip().endswith("}"):
            print "PROBLEM: undended line. Please use parser_prep"
            print yaml_line
            exit()
    
    
    
    # this is a bit hacky, but it works.
    # un-escaped " characters mess the yaml parser up...
    # {value: "this is a "problem""}
    # insted, it's fine if the string looks like this:
    # {value: this is not a problem}
    # solution: (does not work well)
    #yaml_line = yaml_line.replace("\"", "")
    
    
    yaml_line = yaml_line.replace("http:", "http")
    yaml_line = yaml_line.replace("rv:", "rv")
    yaml_line = yaml_line.replace("?", "")
    
    
    #yaml parser might fail :D
    try:
        obj = yaml.load(yaml_line)
    except (yaml.scanner.ScannerError, yaml.parser.ParserError, yaml.reader.ReaderError) as err:
        LOG.error("Parsing yaml to JSON: " + str(yaml_line))
        LOG.error("details: " + str(err))
        #print yaml_line
        #exit()
        return
    valid_json = json.dumps(obj)
    obj = yaml.load(valid_json)
    if not obj:
        LOG.error("Weird error. This line parsed to yaml, not to JSON: " + str(yaml_line))
        return 
    
    #if this is the first time we see this session, add it
    if ('whatismyuri' in obj):
        addSession(current_transaction['IP1'], current_transaction['IP2'])
    
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
        current_transaction['type'] = "$reply"
    
    #INSERT
    elif (insertRegex.match(line)):
        current_transaction['type'] = "$insert"
        line = line[line.find('{'):line.rfind('}')+1]
        add_yaml_to_content(line)
    
    # QUERY
    elif (queryRegex.match(line)):
        current_transaction['type'] = "$query"
        line = line[line.find('{'):line.rfind('}')+1]
        add_yaml_to_content(line)
        
    # UPDATE
    elif (updateRegex.match(line)):
        current_transaction['type'] = "$update"
        #this is hacky, but it's the all I have now
        lines = line[line.find('{'):line.rfind('}')+1] .split(" o:")
        if len(lines) > 2:
            LOG.error("Fuck. This update query is tricky to parse: " + str(line))
            LOG.error("Skipping it for now...")
        if len(lines) < 2:
            return
        add_yaml_to_content(lines[0])
        add_yaml_to_content(lines[1])
    
    # DELETE
    elif (deleteRegex.match(line)):
        current_transaction['type'] = "$delete"
        line = line[line.find('{'):line.rfind('}')+1] 
        add_yaml_to_content(line) 
    
    # GENERIC CONTENT LINE
    else:
        #default: probably just yaml content line...
        add_yaml_to_content(line) 
    return

def parseFile(file):
    LOG.info("Processing file %s...", file)
    file = open(file, 'r')
    line = file.readline()
    ctr = 0
    while line:
        line = file.readline()
        result = headerRegex.match(line)
        #print line
        if result:
            process_header_line(result.groupdict())
            ctr += 1
        else:
            process_content_line(line)

    if (current_transaction):
        store(current_transaction)

    print ""
    LOG.info("Done. Added %d workload sessions to '%s'" % (ctr, workload_col))
        

def main():
    global current_transaction
    global headerRegex

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

    #start connection and set global variables...
    initDB(args['host'], args['port'], args['workload_db'], args['workload_col'])

    settings = "host: ", args['host'], " port: ", args['port'], " file: ", args['file'], " db: ", args['workload_db'], " col: ", args['workload_col']
    LOG.info("Settings: %s", settings)
    
    if args['clean']:
        LOG.warn("Cleaning '%s' collection...", workload_col)
        cleanWorkload()
    
    parseFile(args['file'])
    
    return


if __name__ == '__main__':
        main()



    
