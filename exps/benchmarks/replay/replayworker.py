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

import sys
import os
import string
import re
import logging
import traceback

basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "../../../libs"))
sys.path.append(os.path.join(basedir, "../../../src"))

import pymongo
import mongokit
from pprint import pprint, pformat

# Designer
import workload
import catalog
import search
from util import constants
from util import configutil

from message import *

LOG = logging.getLogger(__name__)

TARGET_DB_NAME = 'replay'

class ReplayWorker:
    def __init__(self, config, channel):
        self.config = config
        self.channel = channel
        self.dataset_db = None
        self.metadata_db = None
        self.collections = None
        
        sendMessage(MSG_INIT_COMPLETED, None, self.channel)
        
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def load(self):
        self.connect2mongodb()
        sendMessage(MSG_INITIAL_DESIGN, None, self.channel)
    ## DEF
    
    def execute(self):
        sendMessage(MSG_START_NOTICE, None, self.channel)
        for sess in self.metadata_db.Session.fetch():
            for op in sess['operations']:
                try:
                    self.executeOperation(op)
                except:
                    LOG.error("Unexpected error when executing operation in Session %s:\n%s" % (sess["_id"], pformat(op)))
                    raise
                ## TRY
            ## FOR
        ## FOR
        
        sendMessage(MSG_EXECUTE_COMPLETED, None, self.channel)
    ## DEF
    
    def connect2mongodb(self):
        hostname = self.config.get(configutil.SECT_MONGODB, 'host')
        port = self.config.getint(configutil.SECT_MONGODB, 'port')
        assert hostname
        assert port
        try:
            conn = mongokit.Connection(host=hostname, port=port)
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
            raise
        ## Register our objects with MongoKit
        conn.register([ catalog.Collection, workload.Session ])

        ## Make sure that the databases that we need are there
        db_names = conn.database_names()
        for key in [ 'dataset_db', ]: # FIXME 'workload_db' ]:
            if not self.config.has_option(configutil.SECT_MONGODB, key):
                raise Exception("Missing the configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
            elif not self.config.get(configutil.SECT_MONGODB, key):
                raise Exception("Empty configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
        ## FOR
        
        self.dataset_db = conn[self.config.get(configutil.SECT_MONGODB, 'dataset_db')]
        self.metadata_db = conn[self.config.get(configutil.SECT_MONGODB, 'metadata_db')]
        self.collections = [col_name for col_name in self.dataset_db.collection_names()]
    ## DEF
    
    def executeOperation(self, op):
        # TODO: We should check whether they are trying to make a query
        # against a collection that we don't know about so that we can print
        # a nice error message. This is because we know that we should never
        # have an operation that doesn't touch a collection that we don't already
        # know about.
        coll = op['collection']
        if not coll in self.collections:
            msg = "Invalid operation on unexpected collection '%s'" % coll
            if coll.find("$cmd"): # MONGODB system error collection
                LOG.warn(op)
                return
            ## IF
            
        if self.debug:
            LOG.debug("Executing '%s' operation on '%s'" % (op['type'], coll))
        
        # QUERY
        if op['type'] == constants.OP_TYPE_QUERY:
            isCount = False
            
            # The query content field has our where clause
            whereClause = op['query_content'][0]['#query']
            
            # And the second element is the projection
            fieldsClause = None
            if 'query_fields' in op and not op['query_fields'] is None:
                fieldsClause = op['query_fields']

            # Check whether this is for a count
            if 'count' in op['query_content'][0]:
                assert "#query" in op['query_content'][0], "OP: " + pformat(op)
                # Then do a count
                whereClause = op['query_content'][0]["#query"]
                isCount = True
                    
            # Execute!
            # TODO: Need to check the performance difference of find vs find_one
            # TODO: We need to handle counts + sorts + limits
            resultCursor = self.dataset_db[coll].find(whereClause, fieldsClause)
            sendMessage(MSG_OP_INFO, (op['collection'], op['type'], whereClause, fieldsClause), self.channel)

            if op["query_limit"] and op["query_limit"] != -1:
                #try:
                resultCursor.limit(op["query_limit"])
                #except:
                    #exit(pformat(op))
                
            if isCount:
                result = resultCursor.count()
            else:
                # We have to iterate through the result so that we know that
                # the cursor has copied all the bytes
                result = [r for r in resultCursor]
            # IF
            
            # TODO: For queries that were originally joins, we need a way
            # to save the output of the queries to use as the input for
            # subsequent queries
            
        # UPDATE
        elif op['type'] == constants.OP_TYPE_UPDATE:
            # The first element in the content field is the WHERE clause
            whereClause = op['query_content'][0]
            assert whereClause, "Missing WHERE clause for %s" % op['type']
            
            # The second element has what we're trying to update
            updateClause = op['query_content'][1]
            assert updateClause, "Missing UPDATE clause for %s" % op['type']
            
            # Let 'er rip!
            # TODO: Need to handle 'upsert' and 'multi' flags
            # TODO: What about the 'manipulate' or 'safe' flags?
            result = self.dataset_db[coll].update(whereClause, updateClause)
            sendMessage(MSG_OP_INFO, (op['collection'], op['type'], whereClause, updateClause), self.channel)
            
        # INSERT
        elif op['type'] == constants.OP_TYPE_INSERT:
            # Just get the payload and fire it off
            # There's nothing else to really do here
            result = self.dataset_db[coll].insert(op['query_content'])
            sendMessage(MSG_OP_INFO, (op['collection'], op['type'], op['query_content'], None), self.channel)
        # DELETE
        elif op['type'] == constants.OP_TYPE_DELETE:
            # The first element in the content field is the WHERE clause
            whereClause = op['query_content'][0]
            assert whereClause, "Missing WHERE clause for %s" % op['type']
            
            # SAFETY CHECK: Don't let them accidently delete the entire collection
            assert len(whereClause) > 0, "SAFETY CHECK: Empty WHERE clause for %s" % op['type']
            
            # I'll see you in hell!!
            result = self.dataset_db[coll].remove(whereClause)
            sendMessage(MSG_OP_INFO, (op['collection'], op['type'], whereClause, None), self.channel)
        
        # UNKNOWN!
        else:
            raise Exception("Unexpected query type: %s" % op['type'])
        
        if self.debug:
            LOG.debug("%s Result: %s" % (op['type'], pformat(result)))
        return result
    ## DEF

## CLASS