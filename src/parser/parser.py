# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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

import re
import logging
from pprint import pformat

LOG = logging.getLogger(__name__)

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
TYPE_QUERY = '$query'
TYPE_INSERT = '$insert'
TYPE_DELETE = '$delete'
TYPE_UPDATE = '$update'
TYPE_REPLY = '$reply'
QUERY_TYPES = [TYPE_QUERY, TYPE_INSERT, TYPE_DELETE, TYPE_UPDATE]

class Parser:
    """Mongosniff Trace Parser"""
    
    def __init__(self, workload_col, fd):
        self.workload_col = workload_col
        self.fd = fd
        self.line_ctr = 0
        self.sess_ctr = 0
        
        self.current_transaction = None
        self.workload_db = None
        self.workload_col = None
        self.recreated_db = None

        # current session map holds all session objects. Mapping client_id --> Session()
        self.current_session_map = {} 
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
        
        pass
    ## DEF
    
    def cleanWorkload(self):
        self.workload_col.remove()
    ## DEF
    
    def parse(self):
        for line in fd:
            self.line_ctr += 1
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
        
        pass
    ## DEF
        
    
## CLASS