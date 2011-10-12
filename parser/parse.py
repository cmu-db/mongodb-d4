#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time
import re
import argparse
import yaml
import json
from pymongo import Connection

###GLOBAL VARS
current_transaction = None
mongo_comm = None

current_session_map = {}
session_uid = 100

INPUT_FILE = "sample.txt"

TIME_MASK = "[0-9]+\.[0-9]+.*"
ARROW_MASK = "(-->>|<<--)"
IP_MASK = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{5,5}"
COLLECTION_MASK = "\w+\.\$?\w+"
SIZE_MASK = "\d+ bytes"
MAGIC_ID_MASK = "id:\w+ \d+"
REPLY_ID_MASK = "\d+"

HEADER_MASK = "(?P<timestamp>" + TIME_MASK + ") *- *" + \
"(?P<IP1>" + IP_MASK + ") *" + \
"(?P<arrow>" + ARROW_MASK + ") *" + \
"(?P<IP2>" + IP_MASK + ") *" + \
"(?P<collection>" + COLLECTION_MASK + ")? *" + \
"(?P<size>" + SIZE_MASK + ") *" + \
"(?P<magic_id>" + MAGIC_ID_MASK + ") *" + \
"-? *(?P<reply_id>" + REPLY_ID_MASK + ")?"
headerRegex = re.compile(HEADER_MASK);

CONTENT_REPLY_MASK = "\s*reply +.*"
CONTENT_INSERT_MASK = "\s*insert: {.*"
CONTENT_QUERY_MASK = "\s*query: {.*"


replyRegex = re.compile(CONTENT_REPLY_MASK)
insertRegex = re.compile(CONTENT_INSERT_MASK)
queryRegex = re.compile(CONTENT_QUERY_MASK)

def initDB(hostname, port):
    global mongo_comm
    connection = Connection(hostname, port)
    db = connection.mongo_designer
    mongo_comm = db.mongo_comm
    return

def store(transaction):
    global mongo_comm
    global current_session_map
    global session_uid

    if (current_transaction['arrow'] == '-->>'):
        ip = current_transaction['IP1']
    else:
        ip = current_transaction['IP2']

    if (ip not in current_session_map):
        current_session_map[ip] = session_uid
        session_uid += 1

    current_transaction['uid'] = current_session_map[ip]
    mongo_comm.insert(transaction)
    return

def process_header_line(header):
    global current_transaction

    if (current_transaction):
        store(current_transaction)

    current_transaction = header
    current_transaction['content'] = []
    return

def process_content_line(line):
    global replyRegex
    global insertRegex
    global queryRegex
    global current_transaction
    global current_session_map
    global session_uid

    if (not current_transaction):
        return

    if (replyRegex.match(line)):
        current_transaction['type'] = "reply"

    elif (insertRegex.match(line)):
        current_transaction['type'] = "insert"
	line = line[line.find('{'):line.rfind('}')+1]

    elif (queryRegex.match(line)):
        current_transaction['type'] = "query"
	line = line[line.find('{'):line.rfind('}')+1]

    obj = yaml.load(line)
    valid_json = json.dumps(obj)
    obj = yaml.load(valid_json)

    if obj:
        if ('whatismyuri' in obj):
            current_session_map[current_transaction['IP1']] = session_uid
	    session_uid += 1        

        current_transaction['content'].append(obj)

    return

def main():
    global current_transaction
    global headerRegex


    aparser = argparse.ArgumentParser(description='MongoSniff Trace Anonymizer')
    aparser.add_argument('hostname',
                         help='hostname of machine running mongo server')
    aparser.add_argument('port', type=int,
                         help='port to connect to')
    args = vars(aparser.parse_args())

    initDB(args['hostname'], args['port'])


    file = open(INPUT_FILE, 'r')
    line = file.readline()
    while line:
        line = file.readline()
        result = headerRegex.match(line)
	print line
        if result:
            process_header_line(result.groupdict())
        else:
            process_content_line(line)

    if (current_transaction):
        store(current_transaction)

    return


if __name__ == '__main__':
	main()



    
