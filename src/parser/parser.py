# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
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

import os
import sys
import re
import yaml
import json
import hashlib
import anonymize # just for hash_string()
import logging
from pprint import pformat

# MongoDB-Designer
sys.path.append("../workload")
from traces import Session

LOG = logging.getLogger(__name__)

## ==============================================
## DEFAULT VALUES
## ==============================================
INITIAL_SESSION_UID = 100 #where to start the incremental session uid

## ==============================================
## PARSING REGEXES
## ==============================================

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
    "(?P<magic_id>" + MAGIC_ID_MASK + ")[\t ]*" + \
    "(?P<trans_id>" + TRANSACTION_ID_MASK + ")[\t ]*" + \
    "-?[\t ]*(?P<query_id>" + REPLY_ID_MASK + ")?"

### content lines
CONTENT_REPLY_MASK = "\s*reply +.*"
CONTENT_INSERT_MASK = "\s*insert: {.*"
CONTENT_QUERY_MASK = "\s*query: {.*"
CONTENT_UPDATE_MASK = "\s*update .*"
CONTENT_DELETE_MASK = "\s*delete .*"

# other masks for parsing
FLAGS_MASK = ".*flags:(?P<flags>\d).*" #vals: 0,1,2,3
NTORETURN_MASK = ".*ntoreturn: (?P<ntoreturn>-?\d+).*" # int 
NTOSKIP_MASK = ".*ntoskip: (?P<ntoskip>\d+).*" #int

# op TYPES
OP_TYPE_QUERY = '$query'
OP_TYPE_INSERT = '$insert'
OP_TYPE_DELETE = '$delete'
OP_TYPE_UPDATE = '$update'
OP_TYPE_REPLY = '$reply'
QUERY_TYPES = [OP_TYPE_QUERY, OP_TYPE_INSERT, OP_TYPE_DELETE, OP_TYPE_UPDATE]

class Parser:
    """Mongosniff Trace Parser"""
    
    def __init__(self, workload_col, fd):
        self.workload_col = workload_col
        self.fd = fd
        self.line_ctr = 0
        self.sess_ctr = 0
        self.recreated_db = None
        
        # The current operation in the session
        self.currentOp = None

        # current session map holds all session objects. Mapping client_id --> Session()
        self.session_map = {} 
        self.session_uid = INITIAL_SESSION_UID # first session_id

        # used to pair up queries & replies by their mongosniff ID
        self.query_response_map = {} 

        # Post-processing global vars. PLAINTEXT Collection Names for AGGREGATES
        # this dictionary is used to figure out the real collection names for aggregate queries
        # the col names are hashed
        # STEP1: during the first pass (the main step of parsing), we store the names of all collections
        # we encounter in the set() known_collections
        # STEP2: we figure out the salt
        # STEP3: we compute the hash. We populate the dict  hashed_collections
        # STEP4: we add the collection names to all aggregate operations
        self.known_collections = set() # set of known collection names
        self.hashed_collections = {} # hash --> collection name
        
        self.headerRegex = re.compile(HEADER_MASK)
        self.replyRegex = re.compile(CONTENT_REPLY_MASK)
        self.insertRegex = re.compile(CONTENT_INSERT_MASK)
        self.queryRegex = re.compile(CONTENT_QUERY_MASK)
        self.updateRegex = re.compile(CONTENT_UPDATE_MASK)
        self.deleteRegex = re.compile(CONTENT_DELETE_MASK)
        
        self.flagsRegex = re.compile(FLAGS_MASK)
        self.ntoreturnRegex = re.compile(NTORETURN_MASK)
        self.ntoskipRegex = re.compile(NTOSKIP_MASK)
        
        assert self.workload_col
        assert self.fd
        pass
    ## DEF

    def getSessionCount(self):
        """Return the number of sessions extracted from the workload trace"""
        return self.sess_ctr
    ## DEF
    
    def cleanWorkload(self):
        """Remove all existing sessions in the workload collection"""
        self.workload_col.remove()
    ## DEF
    
    def getOnlyIP(self, ipAndPort):
        """helper method to split IP and port"""
        # we can be sure that ipAndPort is in the form of IP:port since it was matched by regex...
        return self.ipAndPort.rsplit(":")[0]
    ## DEF
    
    def parse(self):
        """Read each line from the input source and extract all of the sessions"""
        for line in self.fd:
            self.line_ctr += 1
            result = self.headerRegex.match(line)
            #print line
            try:
                if result:
                    self.process_header_line(result.groupdict())
                    self.sess_ctr += 1
                else:
                    self.process_content_line(line)
            except:
                LOG.error("Unexpected error when processing line %d" % self.line_ctr)
                raise
        ## FOR
        if self.currentOp:
            self.storeCurrentOpInSession()
            
        # Post Processing!
        # If only Emanuel was still alive to see this!
        self.infer_aggregate_collections()
        pass
    ## DEF
    
    def addSession(self, ip_client, ip_server):
        """this function initializes a new Session() object (in workload/traces.py)
           and stores it in the collection"""
  
        # ip1 is the key in current_transaction_map
            
        # verify a session with the uid does not exist
        if self.workload_col.find({'session_id': self.session_uid}).count() > 0:
            msg = "Session with UID %s already exists.\n" % self.session_uid
            msg += "Maybe you want to clean the database / use a different collection?"
            raise Exception(msg)

        session = Session()
        session['ip_client'] = unicode(ip_client)
        session['ip_server'] = unicode(ip_server)
        session['session_id'] = self.session_uid
        self.session_map[ip_client] = session
        self.session_uid += 1
        self.workload_col.save(session)
        return session
    ## DEFAULT
    
    def storeCurrentOpInSession(self):
        """Stores the currentOp in a session. We will create a new session if one does not already exist."""
        
        if 'type' not in self.currentOp:
            raise Exception("Current Operation is Incomplete: Missing 'type' field")
        
        # Figure out whether this is a outgoing query from the client
        # Or an incoming response from the server
        if self.currentOp['arrow'] == '-->>':
            ip_client = self.currentOp['IP1']
            ip_server = self.currentOp['IP2']
        else:
            ip_client = self.currentOp['IP2']
            ip_server = self.currentOp['IP1']

        if not ip_client in self.session_map:
            session = self.addSession(ip_client, ip_server)
        else:
            session = self.session_map[ip_client]
        
        # QUERY: $query, $delete, $insert, $update:
        # Create the operation, add it to the session
        if self.currentOp['type'] in QUERY_TYPES:
            # create the operation -- corresponds to current
            query_id = self.currentOp['trans_id'];
            op = {
                'collection': unicode(self.currentOp['collection']),
                'type': unicode(self.currentOp['type']),
                'query_time': float(self.currentOp['timestamp']),
                'query_size': int(self.currentOp['size'].replace("bytes", "")),
                'query_content': self.currentOp['content'],
                'query_id': int(query_id),
                'query_aggregate': 0, # false -not aggregate- by default
            }
            
            # UPDATE Flags
            if op['type'] == OP_TYPE_UPDATE:
                op['update_upsert'] = self.currentOp['update_upsert']
                op['update_multi'] = self.currentOp['update_multi']
            
            # QUERY Flags
            elif op['type'] == OP_TYPE_QUERY:
                # SKIP, LIMIT
                op['query_limit'] = int(self.currentOp['ntoreturn']) # ['ntoreturn']?
                op['query_offset'] = int(self.currentOp['ntoskip']) # ['ntoskip']?
            
                # check for aggregate
                # update collection name, set aggregate type
                if op['collection'].find("$cmd") > 0:
                    op['query_aggregate'] = 1
                    # extract the real collection name
                    ## --> This has to be done at the end after the first pass, because the collection name is hashed up
            
            self.query_response_map[query_id] = op
            # Append it to the current session
            # TODO: Large traces will cause the sessions to get too big.
            #       We need to split out the operations into a seperate collection
            #       Or use multiple sessions
            session['operations'].append(op)
            LOG.debug("Added %s Operation to Session %s:\n%s" % (op['type'], session['session_id'], pformat(op)))
        
            # store the collection name in known_collections. This will be useful later.
            # see the comment at known_collections
            # HACK: We have to cut off the db name here. We may not want
            #       to do that if the application is querying multiple databases.
            full_name = op['collection']
            col_name = full_name[full_name.find(".")+1:] # cut off the db name
            self.known_collections.add(col_name)
        
        # RESPONSE - add information to the matching query
        elif self.currentOp['type'] == OP_TYPE_REPLY:
            query_id = self.currentOp['query_id'];
            # see if the matching query is in the map
            if query_id in self.query_response_map:
                # fill in missing information
                query_op = self.query_response_map[query_id]
                query_op['resp_content'] = self.currentOp['content']
                query_op['resp_size'] = int(self.currentOp['size'].replace("bytes", ""))
                query_op['resp_time'] = float(self.currentOp['timestamp'])
                query_op['resp_id'] = int(self.currentOp['trans_id'])    
            else:
                LOG.warn("SKIPPING RESPONSE (no matching query_id): %s" % query_id)
                
        # UNKNOWN
        else:
            raise Exception("Unexpected message type '%s'" % self.currentOp['type'])
                
        #save the current session
        self.workload_col.save(session)
        
        LOG.debug("session %d was updated" % session['session_id'])
        return
    ## DEF
    
    def process_header_line(self, header):

        if self.currentOp:
            try:
                self.storeCurrentOpInSession()
            except:
                LOG.error("Invalid Session:\n%s" % pformat(self.currentOp))
                raise

        self.currentOp = header
        self.currentOp['content'] = []
        return
    ## DEF
    
    def add_yaml_to_content(self, yaml_line):
        """helper function for process_content_line 
           takes yaml {...} as input and parses the input to JSON and adds that to current['content']"""
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
            self.addSession(current['ip_client'], current['ip_server'])
        
        #store the line
        self.currentOp['content'].append(obj)
        return
    ## DEF

    def process_content_line(self, line):
        """takes any line which does not pass as header line
           tries to figure out the transaction type & store the content"""
        
        # ignore content lines before the first transaction is started
        if not self.currentOp:
            return

        # REPLY
        if self.replyRegex.match(line):
            self.currentOp['type'] = OP_TYPE_REPLY
        
        #INSERT
        elif self.insertRegex.match(line):
            self.currentOp['type'] = OP_TYPE_INSERT
            line = line[line.find('{'):line.rfind('}')+1]
            add_yaml_to_content(line)
        
        # QUERY
        elif self.queryRegex.match(line):
            self.currentOp['type'] = OP_TYPE_QUERY
            
            # extract OFFSET and LIMIT
            self.currentOp['ntoskip'] = self.ntoskipRegex.match(line).groupdict()
            self.currentOp['ntoreturn'] = self.ntoreturnRegex.match(line).groupdict()
            
            line = line[line.find('{'):line.rfind('}')+1]
            self.add_yaml_to_content(line)
            
        # UPDATE
        elif self.updateRegex.match(line):
            self.currentOp['type'] = OP_TYPE_UPDATE
            
            # extract FLAGS
            upsert = False
            multi = False
            flags = self.flagsRegex.match(line).groupdict()
            if flags == '1':
                upsert = True
                multi = False
            elif flags == '2':
                upsert = False
                multi = True
            elif flags == '3':
                upsert = True
                multi = True
            self.currentOp['update_upsert'] = upsert
            self.currentOp['update_multi'] = multi
            
            # extract the CRITERIA and NEW_OBJ
            lines = line[line.find('{'):line.rfind('}')+1].split(" o:")
            if len(lines) > 2:
                LOG.error("Fuck. This update query is tricky to parse: " + str(line))
                LOG.error("Skipping it for now...")
            if len(lines) < 2:
                return
            self.add_yaml_to_content(lines[0])
            self.add_yaml_to_content(lines[1])
        
        # DELETE
        elif self.deleteRegex.match(line):
            self.currentOp['type'] = OP_TYPE_DELETE
            line = line[line.find('{'):line.rfind('}')+1] 
            self.add_yaml_to_content(line) 
        
        # GENERIC CONTENT LINE
        else:
            #default: probably just yaml content line...
            self.add_yaml_to_content(line) 
        ## IF
        
        return
    ## DEF
    
    '''
    Post-processing: infer plaintext collection names for AGGREGATES
    '''
     
    def get_candidate_hashes(self):
        """this functions returns a set of some hashed strings, which are most likely hashed collection names"""
        
        candidate_hashes = set()
        LOG.info("Retrieving hashed collection names...")
        for session in self.workload_col.find():
            for op in session['operations']:
                if op['query_aggregate'] == 1:
                    # find the JSON of the query...
                    query = op['query_content'][0] # we care about the first (0th) BSON in the list
                    # look four count key. This would refer to a collection name
                    if 'count' in query:
                        #print query
                        candidate_hashes.add(query['count'])
        LOG.info("Found %d hashed collection names. " % len(candidate_hashes))
        LOG.debug(candidate_hashes)
        return candidate_hashes
    ## DEF

    def get_hash_string(self, bare_col_name):
        return "\"" + bare_col_name + "\""
    ## DEF

    def infer_salt(self, candidate_hashes, known_collections):
        """this is a ridiculous hack. Let's hope the salt is 0. But even if not..."""
        max_salt = 100000
        LOG.info("Trying to brute-force the salt 0-%d..." % max_salt)
        salt = 0
        while True:
            if salt % (max_salt / 100) == 0:
                print ".",
            for known_col in known_collections:
                hashed_string = self.get_hash_string(known_col) # the col names are hashed with quotes around them 
                hash = anonymize.hash_string(hashed_string, salt) # imported from anonymize.py
                if hash in candidate_hashes:
                    LOG.info("SUCCESS! %s hashes to a known value. SALT: %d", hashed_string, salt)
                    return salt
            salt += 1
            if salt > max_salt:
                break
        LOG.warn("FAIL. The salt value is unknown :(")
        return None
    ## DEF

    def precompute_hashes(self, salt):
        """this function populates the hashed_collections map
           mapping HASHED_COL_NAME -> PLAIN_TEXT_COL_NAME"""
        LOG.info("Precomputing hashes for all known collection names...")
        for col_name in self.known_collections:
            hash = anonymize.hash_string(get_hash_string(col_name), salt)
            self.hashed_collections[hash] = col_name
            LOG.debug("hash: %s / col_name: %s / hash_str: %s" % (hash, col_name, get_hash_string(col_name)))
        ## FOR
        LOG.info("Done.")
    ## DEF

    # now we go through aggregate ops again and fill in the collection name...
    def fill_aggregate_collection_names(self):
        LOG.info("Adding plaintext collection names to aggregate operations...")
        cnt = 0
        for session in self.workload_col.find():
            for op in session['operations']:
                if op['query_aggregate'] == 1:
                    query = op['query_content'][0] # first and only JSON from the payload
                    # iterate through the keys in the query JSON
                    # one of the should point to the hashed collection name
                    for key in query:
                        value = query[key]
                        #print "value: ", value, " type: ", type(value)
                        if type(value) is unicode:
                            #print "candidate val: ", value
                            if value in hashed_collections:
                                # YES. We found it!
                                # contains $cmd. Just to double-check
                                if op['collection'].find("$cmd") < 0:
                                    LOG.warn("Aggregate operation does not seem to be aggregate. Skipping.")
                                    LOG.debug(pformat(op))
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
            self.workload_col.save(session)
        ### for
        LOG.info("Done. Updated %d aggregate operations." % cnt)
    ## DEF

    # CALL THIS FUNCTION TO DO THE POST-PROCESSING
    def infer_aggregate_collections(self):
        LOG.info("")
        LOG.info("-- Aggregate Collection Names --")
        LOG.info("Encountered %d collection names in plaintext." % len(known_collections))
        LOG.debug(pformat(self.known_collections))
        candidate_hashes = self.get_candidate_hashes()
        salt = self.infer_salt(candidate_hashes, self.known_collections)
        if salt is None:
            return
        self.precompute_hashes(salt)
        self.fill_aggregate_collection_names()
    ## DEF

    '''
    END OF Post-processing: AGGREGATE collection names
    '''
    
    
## CLASS