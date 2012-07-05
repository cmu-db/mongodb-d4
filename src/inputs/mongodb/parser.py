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
from bson.errors import InvalidDocument
import copy

import os
import sys
import re
import yaml
import json
import logging
from pprint import pformat

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# mongodb-d4
sys.path.append(os.path.join(basedir, ".."))
import util
from sanitizer import anonymize
from workload import Session
from workload import OpHasher
from util import constants

LOG = logging.getLogger(__name__)

## ==============================================
## PARSING REGEXES
## ==============================================

### parts of header
TIME_MASK = "[0-9]+\.[0-9]+"
ARROW_MASK = "(?:-->>|<<--)"
IP_MASK = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,}"

# HACK: The trace from ex.fm has mangled unicode characters for collection
#       names, so we have to add in non-standard characters
COLLECTION_MASK = ".*?"
#COLLECTION_MASK = "([\w]+[\.\}]){1,}\$?[\w]+[\)]?" 

SIZE_MASK = "\d+"
MAGIC_ID_MASK = "id:\w+"
QUERY_ID_MASK = "\d+"
REPLY_ID_MASK = "\d+"

### header
HEADER_MASK = "(?P<timestamp>" + TIME_MASK + ")[\s]+[\-][\s]+" + \
    "(?P<ip1>" + IP_MASK + ")[\s]*" + \
    "(?P<arrow>" + ARROW_MASK + ")[\s]*" + \
    "(?P<ip2>" + IP_MASK + ")[\s]+" + \
    "(?P<collection>" + COLLECTION_MASK + ")[\s]+" + \
    "(?P<size>" + SIZE_MASK + ") bytes[\s]+" + \
    "(?P<magic_id>" + MAGIC_ID_MASK + ")[\t\s]+" + \
    "(?P<query_id>" + QUERY_ID_MASK + ")[\t\s]*" + \
    "(?:-[\t\s]*(?P<reply_id>" + REPLY_ID_MASK + "))?"

### content lines
CONTENT_REPLY_MASK = "\s*reply +.*"
CONTENT_INSERT_MASK = "\s*insert: {.*"
CONTENT_QUERY_MASK = "\s*query: {.*"
CONTENT_UPDATE_MASK = "\s*update .*"
CONTENT_DELETE_MASK = "\s*delete .*"
CONTENT_ERROR_MASK = "\s*[A-Z]\w{2} [\w]+ [\d]{1,2} [\d]{2}:[\d]{2}:[\d]{2} Assertion: .*?"

# other masks for parsing
FLAGS_MASK = ".*flags:(?P<flags>\d).*" #vals: 0,1,2,3
NTORETURN_MASK = ".*ntoreturn: (?P<ntoreturn>-?\d+).*" # int 
NTOSKIP_MASK = ".*ntoskip: (?P<ntoskip>\d+).*" #int

# Original Code: Emanuel Buzek
class Parser:
    """Mongosniff Trace Parser"""
    
    def __init__(self, metadata_db, fd):
        self.metadata_db = metadata_db
        self.fd = fd
        self.line_ctr = 0
        self.resp_ctr = 0
        self.skip_ctr = 0
        self.sess_ctr = 0
        self.op_ctr = 0
        self.op_skip = None
        self.sess_limit = None
        self.op_limit = None
        self.recreated_db = None
        self.stop_on_error = False

        # If set to true, then we will periodically save the Sessions out to the
        # metatdata_db in case we crash and want to inspect what happened
        self.save_checkpoints = True
        
        # This is used to generate deterministic identifiers
        # for similar operations
        self.opHasher = OpHasher()
        
        # If this flag is true, then mongosniff got an invalid message packet
        # So we'll skip until we find the next matching header
        self.skip_to_next = False
        
        # The current operation in the session
        self.currentContent = [ ]
        self.currentOp = None

        # Operations with a busted collection name
        # Once we get parse all of the operations, we'll go back and
        # try to figure out what collection that might be referring to
        # based on the keys that they reference
        self.bustedOps = [ ]
        
        # current session map holds all session objects. Mapping client_id --> Session
        self.session_map = {} 
        self.next_session_id = constants.INITIAL_SESSION_ID # first session_id

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
        
        self.headerRegex = re.compile(HEADER_MASK, re.UNICODE)
        self.replyRegex = re.compile(CONTENT_REPLY_MASK)
        self.insertRegex = re.compile(CONTENT_INSERT_MASK)
        self.queryRegex = re.compile(CONTENT_QUERY_MASK)
        self.updateRegex = re.compile(CONTENT_UPDATE_MASK)
        self.deleteRegex = re.compile(CONTENT_DELETE_MASK)
        self.errorRegex = re.compile(CONTENT_ERROR_MASK)
        
        self.flagsRegex = re.compile(FLAGS_MASK)
        self.ntoreturnRegex = re.compile(NTORETURN_MASK)
        self.ntoskipRegex = re.compile(NTOSKIP_MASK)

        self.debug = LOG.isEnabledFor(logging.DEBUG)

        assert self.metadata_db
        assert self.fd
        pass
    ## DEF

    def getSessionCount(self):
        """Return the number of sessions extracted from the workload trace"""
        return len(self.session_map)
    ## DEF
    
    def getOpCount(self):
        """Return the number of operations extracted from the workload trace"""
        return self.op_ctr
    ## DEF
    
    def getOpSkipCount(self):
        """Return the number of operations that were skipped during processing"""
        return self.skip_ctr
    ## DEF
    
    def clean(self):
        """Remove all existing sessions in the workload collection"""
        LOG.warn("Purging existing sessions collection '%s.%s'" % (self.metadata_db.name, self.metadata_db.Session))
        self.metadata_db.Session.delete()
    ## DEF
    
    def getOnlyIP(self, ipAndPort):
        """helper method to split IP and port"""
        # we can be sure that ipAndPort is in the form of IP:port since it was matched by regex...
        return self.ipAndPort.rsplit(":")[0]
    ## DEF
    
    def process(self):
        """Read each line from the input source and extract all of the sessions"""
        for line in self.fd:
            self.line_ctr += 1
            if self.op_skip and self.line_ctr < self.op_skip:
                continue
            
            try:
                # Parse the current line to decide whether this 
                # is the beginning of a new operaton/reply
                result = self.headerRegex.match(line)
                if result:
                    self.skip_to_next = False
                    self.process_header_line(result.groupdict())
                elif not self.skip_to_next:
                    self.process_content_line(line)
            except:
                LOG.error("Unexpected error when processing line %d" % self.line_ctr)
                raise
            finally:
                # Checkpoint!
                if self.save_checkpoints and self.line_ctr % 10000 == 0:
                    self.saveSessions(self.session_map.itervalues())

            if not self.sess_limit is None and self.sess_ctr >= self.sess_limit:
                LOG.warn("Session Limit Reached. Halting processing [sess_limit=%d]", self.sess_limit)
                break
            elif not self.op_limit is None and self.op_ctr >= self.op_limit:
                LOG.warn("Operation Limit Reached. Halting processing [op_limit=%d]", self.op_limit)
                break
        ## FOR
        if self.currentOp:
            self.storeCurrentOpInSession()
            
        # Post Processing!
        # If only Emanuel was still alive to see this!
        self.postProcess()
        
        self.saveSessions(self.session_map.itervalues())
    ## DEF
    
    def saveSessions(self, sessions, noSplit=False):
        """Save all sessions!"""
        for session in sessions:
            if not len(session['operations']):
                if self.debug:
                    LOG.warn("Ignoring Session %(session_id)d because it doesn't have any operations" % session)
                continue
            try:
                session.save()
            except (mongokit.MaxDocumentSizeError, InvalidDocument) as err:
                if noSplit: raise
                self.splitSession(session)
                pass
            except Exception as err:
                dump = pformat(session)
                #if len(dump) > 1024: dump = dump[:1024].strip() + "..."
                LOG.error("Failed to save session: %s\n%s" % (err.message, dump))
                raise
        ## FOR
    ## DEF

    def storeCurrentOpInSession(self):
        """Stores the currentOp in a session. We will create a new session if one does not already exist."""
        
        # Check whether it has a busted collection name
        # For now we'll just change the name to our marker so that we can figure out
        # what it really should be after we recreate the schema
        try:
            self.currentOp['collection'].decode('ascii')
        except Exception as err:
            if self.debug:
                LOG.warn("Operation %(query_id)d has an invalid collection name '%(collection)s'. Will fix later... [opCtr=%(op_ctr)d / lineCtr=%(line_ctr)d]" % self.currentOp)
            self.currentOp['collection'] = constants.INVALID_COLLECTION_MARKER
            self.bustedOps.append(self.currentOp)
            pass
        
        # Figure out whether this is a outgoing query from the client
        # Or an incoming response from the server
        if self.currentOp['arrow'] == '-->>':
            ip_client = self.currentOp['ip1']
            ip_server = self.currentOp['ip2']
        else:
            ip_client = self.currentOp['ip2']
            ip_server = self.currentOp['ip1']
            
            # If this doesn't have a type here, then we know that it's a reply
            if not 'type' in self.currentOp:
                self.currentOp['type'] = constants.OP_TYPE_REPLY
        ## IF

        if not 'type' in self.currentOp:
            msg = "Current operation is incomplete on line %d: Missing 'type' field" % self.line_ctr
            LOG.warn("%s [opCtr=%d]\n%s" % (msg, self.op_ctr, pformat(self.currentOp)))
            if self.stop_on_error: raise Exception(msg)
            return
        ## IF
        
        # Get the session to store this operation in
        session = self.getOrCreateSession(ip_client, ip_server)
        
        # Escape any invalid key names
        for i in xrange(0, len(self.currentContent)):
            # HACK: Rename the 'query' key to '$query'
            if 'query' in self.currentContent[i]:
                self.currentContent[i][constants.OP_TYPE_QUERY] = self.currentContent[i]['query']
                del self.currentContent[i]['query']
            self.currentContent[i] = util.escapeFieldNames(self.currentContent[i])
        ## FOR
        
        # QUERY: $query, $delete, $insert, $update:
        # Create the operation, add it to the session
        if self.currentOp['type'] in [constants.OP_TYPE_QUERY, constants.OP_TYPE_INSERT, constants.OP_TYPE_DELETE, constants.OP_TYPE_UPDATE]:
            # create the operation -- corresponds to current
            if self.debug:
                LOG.debug("Current Operation %d Content:\n%s" % (self.currentOp['query_id'], pformat(self.currentContent)))
            
            op = Session.operationFactory()
            op['collection']        = self.currentOp['collection']
            op['type']              = self.currentOp['type']
            op['query_time']        = self.currentOp['timestamp']
            op['query_size']        = self.currentOp['size']
            op['query_content']     = self.currentContent
            op['query_id']          = long(self.currentOp['query_id'])
            op['query_aggregate']   = False # false -not aggregate- by default

            # UPDATE Flags
            if op['type'] == constants.OP_TYPE_UPDATE:
                op['update_upsert'] = self.currentOp['update_upsert']
                op['update_multi'] = self.currentOp['update_multi']
            
            # QUERY Flags
            elif op['type'] == constants.OP_TYPE_QUERY:
                # SKIP, LIMIT
                op['query_limit'] = self.currentOp['ntoreturn']
                op['query_offset'] = self.currentOp['ntoskip']
            
                # check for aggregate
                # update collection name, set aggregate type
                if op['collection'].find("$cmd") > 0:
                    op['query_aggregate'] = True
                    # extract the real collection name
                    ## --> This has to be done at the end after the first pass, because the collection name is hashed up
            
            # Keep track of operations by their ids so that we can add
            # the response to it later on
            self.query_response_map[self.currentOp['query_id']] = op
            
            # Always add the query_hash
            try:
                op['query_hash'] = self.opHasher.hash(op)
            except Exception:
                msg = "Failed to compute hash on operation\n%s" % pformat(op)
                if self.debug: LOG.warn(msg)
                if self.stop_on_error: raise Exception(msg)
                return
            
            # Append it to the current session
            # TODO: Large traces will cause the sessions to get too big.
            #       We need to split out the operations into a seperate collection
            #       Or use multiple sessions
            session['operations'].append(op)
            self.op_ctr += 1
            if self.debug:
                LOG.debug("Added %s operation %d to session %s from line %d:\n%s" % (op['type'], self.currentOp['query_id'], session['session_id'], self.line_ctr, pformat(op)))
        
            # store the collection name in known_collections. This will be useful later.
            # see the comment at known_collections
            # HACK: We have to cut off the db name here. We may not want
            #       to do that if the application is querying multiple databases.
            full_name = op['collection']
            col_name = full_name[full_name.find(".")+1:] # cut off the db name
            self.known_collections.add(col_name)
        
        # RESPONSE - add information to the matching query
        elif self.currentOp['type'] == constants.OP_TYPE_REPLY:
            self.resp_ctr += 1
            reply_id = self.currentOp['reply_id'];
            # see if the matching query is in the map
            if reply_id in self.query_response_map:
                # fill in missing information
                query_op = self.query_response_map[reply_id]
                query_op['resp_content'] = self.currentContent
                query_op['resp_size'] = self.currentOp['size']
                query_op['resp_time'] = self.currentOp['timestamp']
                query_op['resp_id'] = long(self.currentOp['query_id'])
                del self.query_response_map[reply_id]
            else:
                self.skip_ctr += 1
                if self.debug:
                    LOG.warn("Skipping response on line %d - No matching query_id '%s' [skipCtr=%d/%d]" % (self.line_ctr, reply_id, self.skip_ctr, self.resp_ctr))
                
        # These can be safely ignored
        elif self.currentOp['type'] in [constants.OP_TYPE_GETMORE, constants.OP_TYPE_KILLCURSORS]:
            if self.debug:
                LOG.warn("Skipping '%s' operation %d on line %d" % (self.currentOp['type'], self.currentOp['query_id'], self.line_ctr))
            
        # UNKNOWN
        else:
            raise Exception("Unexpected message type '%s'" % self.currentOp['type'])
                
        return
    ## DEF

    def splitSession(self, orig_sess):
        # HACK: If the number of operations in the session gets too big, then
        # we have to spill the session into another object.
        # This kind of messes up

        new_sess = self.metadata_db.Session()
        for k in orig_sess.iterkeys():
            if not k in ['operations', 'session_id']:
                new_sess[k] = copy.deepcopy(orig_sess[k])
                ## FOR
        new_sess['session_id'] = self.__nextSessionId__()

        # Split the operations list in half
        num_operations = len(orig_sess['operations'])
        split = num_operations / 2
        new_sess['operations'] = orig_sess['operations'][split:]
        orig_sess['operations'] = orig_sess['operations'][:split]

        # Keep track of how we split them in case we want to combine them
        # later on when we sessionize the workload
        new_sess['parent_session_id'] = orig_sess['session_id']

        # Try to save the original session again
        self.saveSessions([orig_sess], noSplit=True)

        # Now update the session map so that the next time that we need
        # to add an operation for this client we get our new Session
        self.session_map[orig_sess['ip_client']] = new_sess

        if self.debug:
            LOG.debug("Split Session #%d with %d operations into new Session #%d", \
                      orig_sess['session_id'], num_operations, new_sess['session_id'])
    ## DEF

    def getOrCreateSession(self, ip_client, ip_server):
        """this function initializes a new Session() object (in workload/traces.py)
           and stores it in the collection"""
  
        # ip1 is the key in current_transaction_map
        if ip_client in self.session_map:
            session = self.session_map[ip_client] 
        else:
            # verify a session with the same id does not exist
            if self.metadata_db.Session.fetch({'session_id': self.next_session_id}).count() > 0:
                msg = "Session with UID %s already exists.\n" % self.next_session_id
                msg += "Maybe you want to clean the database / use a different collection?"
                raise Exception(msg)

            session = self.metadata_db.Session()
            session['ip_client'] = unicode(ip_client)
            session['ip_server'] = unicode(ip_server)
            session['session_id'] = self.__nextSessionId__()
            self.session_map[ip_client] = session
        ## IF
        
        return session
    ## DEF

    def __nextSessionId__(self):
        _id = self.next_session_id
        self.next_session_id += 1
        self.sess_ctr += 1
        return _id
    ## DEF
    
    def process_header_line(self, header):
        # If we already have a currentOp, then we know that 
        # we have finished processing all of its content and we should
        # store it in a session
        if self.currentOp:
            try:
                self.storeCurrentOpInSession()
            except:
                LOG.error("Invalid Session:\n%s" % pformat(self.currentOp))
                raise
        
        self.currentOp = header
        self.currentContent = []
        
        # Add in the op_ctr for debugging purposes
        self.currentOp["op_ctr"] = self.op_ctr
        self.currentOp["line_ctr"] = self.line_ctr
        
        # Fix field types
        for f in ['size', 'query_id', 'reply_id']:
            if f in self.currentOp and self.currentOp[f] != None:
                self.currentOp[f] = int(self.currentOp[f])
        for f in ['timestamp']:
            if f in self.currentOp:
                self.currentOp[f] = float(self.currentOp[f])
        
        # Check whether this is an operation on a collection that we're suppose
        # to ignore
        if 'collection' in self.currentOp:
            col_name = self.currentOp['collection']
            prefix = col_name.split('.')[0]
            if prefix in constants.IGNORED_COLLECTIONS:
                if self.debug:
                    LOG.debug("Ignoring operation %(query_id)d on collection '%(collection)s' [opCtr=%(op_ctr)d / lineCtr=%(line_ctr)d]" % self.currentOp)
                self.skip_to_next = True
                self.currentOp = None
        ## IF
        
        return
    ## DEF
    
    def add_yaml_to_content(self, yaml_line):
        """helper function for process_content_line 
           takes yaml {...} as input and parses the input to JSON and adds that to currentContent"""
        yaml_line = yaml_line.strip()
        
        # Skip empty lines
        if len(yaml_line) == 0:
            return
            
        # Check whether this is a "getMore" message
        elif yaml_line.startswith("getMore"):
            self.currentOp['type'] = constants.OP_TYPE_GETMORE
            return
        # Check whether this is a "killCursors" message
        elif yaml_line.startswith("killCursors"):
            self.currentOp['type'] = constants.OP_TYPE_KILLCURSORS
            return

        # this is not a content line... it can't be yaml
        elif not yaml_line.startswith("{"):
            msg = "Invalid Content on Line %d: JSON does not start with '{'" % self.line_ctr
            LOG.debug("Offending Line: %s" % yaml_line)
            if self.stop_on_error: raise Exception(msg)
            LOG.warn(msg)
            return
        elif not yaml_line.endswith("}"):
            msg = "Invalid Content on Line %d: JSON does not end with '}'" % self.line_ctr
            LOG.debug(yaml_line)
            if self.stop_on_error: raise Exception(msg)
            LOG.warn(msg)
            return    
        
        #yaml parser might fail :D
        try:
            obj = yaml.load(yaml_line)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError, yaml.reader.ReaderError) as err:
            msg = "Invalid Content on Line %d: Failed to convert YAML to JSON:\n%s" % (self.line_ctr, yaml_line)
            if self.stop_on_error: raise Exception(msg)
            LOG.warn(msg)
            return
        
        valid_json = json.dumps(obj)
        obj = yaml.load(valid_json)
        if not obj:
            msg = "Invalid Content on Line %d: Content parsed to YAML, not to JSON\n%s" % (self.line_ctr, yaml_line)
            if self.stop_on_error: raise Exception(msg)
            LOG.warn(msg)
            return
        
        # If this is the first time we see this session, add it
        # TODO: Do we still need this?
        if 'whatismyuri' in obj:
            self.getOrCreateSession(self.currentOp['ip_client'], this.currentOp['ip_server'])
        
        # Store the line in the curentContent buffer
        self.currentContent.append(obj)
        return
    ## DEF

    def process_content_line(self, line):
        """takes any line which does not pass as header line
           tries to figure out the transaction type & store the content"""
        
        # ignore content lines before the first transaction is started
        if not self.currentOp:
            return
            
        # Ignore anything that looks like an error from mongosniff

        # REPLY
        if self.replyRegex.match(line):
            self.currentOp['type'] = constants.OP_TYPE_REPLY
        
        # INSERT
        elif self.insertRegex.match(line):
            self.currentOp['type'] = constants.OP_TYPE_INSERT
            line = line[line.find('{'):line.rfind('}')+1]
            self.add_yaml_to_content(line)
        
        # QUERY
        elif self.queryRegex.match(line):
            self.currentOp['type'] = constants.OP_TYPE_QUERY
            
            # extract OFFSET and LIMIT
            self.currentOp['ntoskip'] = int(self.ntoskipRegex.match(line).group(1))
            self.currentOp['ntoreturn'] = int(self.ntoreturnRegex.match(line).group(1))
            
            line = line[line.find('{'):line.rfind('}')+1]
            self.add_yaml_to_content(line)
            
        # UPDATE
        elif self.updateRegex.match(line):
            self.currentOp['type'] = constants.OP_TYPE_UPDATE
            
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
            self.currentOp['type'] = constants.OP_TYPE_DELETE
            line = line[line.find('{'):line.rfind('}')+1] 
            self.add_yaml_to_content(line) 
        
        # ERROR
        elif self.errorRegex.match(line):
            self.skip_to_next = True
        
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
    
    def postProcess(self):
        """Process the operations to fix the collection names used in aggregate queries"""
        
        if not self.known_collections:
            LOG.warn("No plaintext collections were found in operations. Unable to perform post-processing")
            return
        
        LOG.info("Performing post processing on %s sessions with %d operations" % (self.getSessionCount(), self.getOpCount()))
        if self.debug:
            LOG.debug("-- Aggregate Collection Names --")
            LOG.debug("Encountered %d collection names in plaintext." % len(self.known_collections))
            LOG.debug(pformat(self.known_collections))
        
        # Find 
        candidate_hashes = self.get_candidate_hashes()
        
        # HACK: Figure out what salt was used so that we can match
        #       them with our known collection names
        salt = self.infer_salt(candidate_hashes, self.known_collections)
        if salt is None:
            LOG.warn("Failed to find string hashing salt. Unable to fix aggregate collection names")
            return

        # Now for the given salt value, populate a mapping from
        # hashes to collection names
        LOG.debug("Pre-computing hashes for all known collection names...")
        hashed_collections = {} # hash --> collection name
        for col_name in self.known_collections:
            hash = anonymize.hash_string(self.get_hash_string(col_name), salt)
            hashed_collections[hash] = col_name
            if self.debug:
                LOG.debug("hash: %s / col_name: %s / hash_str: %s" % (hash, col_name, get_hash_string(col_name)))
        ## FOR
            
        # Now use our hash xref to fix the collection names in all aggreate operations
        self.fix_collection_names(hashed_collections)
    ## DEF
    
    def get_candidate_hashes(self):
        """this functions returns a set of some hashed strings, which are most likely hashed collection names"""
        
        candidate_hashes = set()
        LOG.info("Retrieving hashed collection names...")
        for session in self.session_map.itervalues():
            for op in session['operations']:
                if op['query_aggregate']:
                    # find the JSON of the query...
                    # we care about the first (0th) BSON in the list
                    query = op['query_content'][0] 
                    
                    # The 'count' key corresponds to the target collection name
                    if 'count' in query:
                        #print query
                        candidate_hashes.add(query['count'])
        ## FOR
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

    def fix_collection_names(self, hashed_collections):
        """now we go through aggregate ops again and fill in the collection name..."""
        
        if self.debug:
            LOG.debug("Adding plaintext collection names to hashed operations...")
            LOG.debug("PlainText Hashes:\n%s" % pformat(hashed_collections))
        
        cnt = 0
        for session in self.session_map.itervalues():
            dirty = False
            for op in session['operations']:
                query = op['query_content'][0]
                if op['query_aggregate'] or 'findAndModify' in query:
                    if self.debug:
                        LOG.debug("Fixable Operation Candidate:\n%s" % pformat(op))
                    
                    # first and only JSON from the payload
                    # iterate through the keys in the query JSON
                    # one of the should point to the hashed collection name
                    for key,value in query.iteritems():
                        if type(value) in [unicode, str] and value in hashed_collections:
                            # YES. We found it!
                            # Make sure that the original collection name contains $cmd
                            if op['collection'].find("$cmd") < 0:
                                LOG.warn("Unexpected operation")
                                LOG.debug(pformat(op))
                                continue
                            
                            # the plaintext collection name is restored
                            orig_name = op['collection']
                            col_name = hashed_collections[value] 
                            query[key] = col_name
                            
                            # extract the db name from db.$cmd
                            op['collection'] = "%s.%s" % (orig_name.split(".")[0], col_name)
                            
                            LOG.debug("Fixed Operation %d: %s -> %s" % (op['query_id'], value, op['collection']))
                            dirty = True
                        ## IF
                ## IF
            ## FOR (operations)
            
            # save the session if it was changed
            if dirty: 
                session.save()
                cnt += 1
        ## FOR (sessions)
        LOG.info("Done. Fixed %d operations." % cnt)
    ## DEF


    '''
    END OF Post-processing: AGGREGATE collection names
    '''
    
    
## CLASS