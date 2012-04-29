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
import pymongo
import mongokit
from pprint import pprint, pformat

# Designer
import workload
from util import constants

# Benchmark
from api.abstractworker import AbstractWorker
from api.message import *

LOG = logging.getLogger(__name__)

TARGET_DB_NAME = 'replay'

class ReplayWorker(AbstractWorker):
    
    def initImpl(self, config):
        # We need two connections. One to the target database and one
        # to the workload database. 
        
        self.ignoreCollections = config[self.name]['ignorecollections']
        
        ## ----------------------------------------------
        ## WORKLOAD REPLAY CONNECTION
        ## ----------------------------------------------
        self.replayConn = None
        self.replayHost = config[self.name]['host']
        self.replayPort = config[self.name]['port']
        try:
            self.replayConn = mongokit.Connection(host=self.replayHost, port=self.replayPort)
        except:
            LOG.error("Failed to connect to replay MongoDB at %s:%s" % (self.replayHost, self.replayPort))
            raise
        assert self.replayConn
        self.workloadDB = self.replayConn[config[self.name]['workloaddb']]
        self.dataDB = self.replayConn[config[self.name]['datadb']]
        self.collections = [c for c in self.dataDB.collection_names()]
        self.collections.remove("system.indexes")
        LOG.debug("Original Data Collections: %s" % self.collections)
        
        ## ----------------------------------------------
        ## TARGET DATABASE
        ## ----------------------------------------------
        self.targetDB = self.conn[TARGET_DB_NAME]
        
        # TODO: Figure out we're going to load the database. I guess it
        # would come from the workload database, right?
        if config['default']["reset"]:
            LOG.info("Resetting database '%s'" % TARGET_DB_NAME)
            self.conn.drop_database(TARGET_DB_NAME)
        
        # TODO: We need to also load in the JSON design file generated
        # by the designer tool.

        # TODO: We are going to need to examine each session and figure out whether
        # we need to combine operations together if they access collections that
        # are denormalized into each other
        self.replayCursor = self.workloadDB[config[self.name]['workloadcollection']].find({'operations': {'$ne': {'$size': 0}}})
       
        return  
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        assert self.conn
        from bson.code import Code
        
        # TODO: Get the original database from the replayConn and then
        # massage it according to the design
        copied = [ ]
        denormalized = False
        for collName in self.collections:
            # TODO: Examine the design and see whether this collection
            # is denormalized into another collection
            
            if denormalized:
                # TODO: Check whether the collection that we're suppose to copy
                # ourselves into has already been copied over
                pass
                
            # Otherwise we can just copy it directly
            else:
                LOG.debug("Copying collection %s.%s to %s.%s" % (self.dataDB.name, collName, self.targetDB.name, collName))
              
                # I look into trying use server-side code for copying the 
                # collection but I think the Javascript stuff is sandboxed
                # into a single database
              
                batch = [ ]
                ctr = 0
                for d in self.dataDB[collName].find():
                    batch.append(d)
                    if len(batch) == 1000: # FIXME
                        self.targetDB[collName].insert(batch)
                        batch = [ ]
                    ctr += 1
                ## FOR
                if len(batch) > 0:
                    self.targetDB[collName].insert(batch)
                copied.append(collName)
                LOG.debug("Finished copying '%s' [numDocuments=%d]" % (collName, ctr))
        ## FOR
                
        
        
        # TODO: Go through the sample workload and rewrite the queries according
        # to the design. I think that this just means that we need to identify whether
        # we have any denormalized collections.

    ## DEF
    
    def next(self, config):
        assert self.replayCursor
        
        # I guess if we run out of Sessions we can just loop back around?
        try:
            sess = self.replayCursor.next()
        except:
            raise
            #self.replaySessionIdx = 0
            #return self.next(config)
        #self.replaySessionIdx += 1
        
        # It would be nice if had a classification for these 
        # sessions so that we could actually know what we are doing here
        if self.debug:
            LOG.debug("Next Session '%s' / %d Operations" % (sess["_id"], len(sess["operations"])))
        return ("replay", sess)
    ## DEF
        
    def executeImpl(self, config, txn, sess):
        """Execute the operations for the given session"""
        assert self.conn
        assert sess
        
        if self.debug:
            LOG.debug("START (%s) %s" % (sess["_id"], "-"*50))
        for op in sess['operations']:
            try:
                self.executeOperation(op)
            except:
                LOG.error("Unexpected error when executing operation in Session %s:\n%s" % (sess["_id"], pformat(op)))
                raise
        ## FOR
        if self.debug:
            LOG.debug("FINISH (%s) %s" % (sess["_id"], "-"*50))
        
        return
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
            if self.ignoreCollections:
                LOG.warn(msg)
                return
            else:
                raise Exception(msg)
        if self.debug:
            LOG.debug("Executing '%s' operation on '%s'" % (op['type'], coll))
        
        # QUERY
        if op['type'] == "$query":
            # The query field has our where clause
            whereClause = op['content'][0]['query']
            
            # And the second element is the projection
            fieldsClause = None
            if 'fields' in op['content']:
                fieldsClause = op['content']['fields']

            # Execute!
            # TODO: Need to check the performance difference of find vs find_one
            # TODO: We need to handle counts + sorts + limits
            if self.debug:
                LOG.debug("%s '%s' - WHERE:%s - FIELDS:%s" % (op['type'][1:].upper(), coll, whereClause, fieldsClause))
            resultCursor = self.targetDB[coll].find(whereClause, fieldsClause)
            
            # We have to iterate through the result so that we know that
            # the cursor has copied all the bytes
            result = [r for r in resultCursor]
            LOG.debug("Number of Results: %d" % len(result))
            
            # TODO: For queries that were originally joins, we need a way
            # to save the output of the queries to use as the input for
            # subsequent queries
            
        # UPDATE
        elif op['type'] == "$update":
            # The first element in the content field is the WHERE clause
            whereClause = op['content'][0]
            assert whereClause, "Missing WHERE clause for %s" % op['type']
            
            # The second element has what we're trying to update
            updateClause = op['content'][1]
            assert updateClause, "Missing UPDATE clause for %s" % op['type']
            
            # Let 'er rip!
            # TODO: Need to handle 'upsert' and 'multi' flags
            # TODO: What about the 'manipulate' or 'safe' flags?
            if self.debug:
                LOG.debug("%s '%s' - WHERE:%s - FIELDS:%s" % (op['type'][1:].upper(), coll, whereClause, updateClause))
            result = self.targetDB[coll].update(whereClause, updateClause)
            
        # INSERT
        elif op['type'] == "$insert":
            # Just get the payload and fire it off
            # There's nothing else to really do here
            result = self.targetDB[coll].insert(op['content'])
            
        # DELETE
        elif op['type'] == "$delete":
            # The first element in the content field is the WHERE clause
            whereClause = op['content'][0]
            assert whereClause, "Missing WHERE clause for %s" % op['type']
            
            # SAFETY CHECK: Don't let them accidently delete the entire collection
            assert len(whereClause) > 0, "SAFETY CHECK: Empty WHERE clause for %s" % op['type']
            
            # I'll see you in hell!!
            if self.debug:
                LOG.debug("%s '%s' - WHERE:%s" % (op['type'][1:].upper(), coll, whereClause))
            result = self.targetDB[coll].remove(whereClause)
        
        # UNKNOWN!
        else:
            raise Exception("Unexpected query type: %s" % op['type'])
        
        if self.debug:
            LOG.debug("%s Result: %s" % (op['type'], pformat(result)))
        return result
    ## DEF

## CLASS