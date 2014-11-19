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

from pprint import pprint, pformat

# Designer
from util import constants

from api.abstractworker import AbstractWorker
from api.message import *

LOG = logging.getLogger(__name__)

class ReplayWorker(AbstractWorker):
    def __init__(self):
        AbstractWorker.__init__(self)
        self.metadata_db = None
        self.dataset_db = None
        self.replayCursor = None
    ## DEF

    def initImpl(self, config, msg):
        pass
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        pass
    ## DEF
    
    def __rewind_cursor__(self):
        self.replayCursor = self.metadata_db.sessions.find()
        #print self.data
        #self.replayCursor = self.metadata_db.sessions.find({'session_id':{'$gte':self.data[0],'$lt':self.data[1]}})
    ## DEF

    ## DEF 
    @staticmethod
    def processDollarReplacement(key):
        newkey = ''
        if key[0] == constants.REPLACE_KEY_DOLLAR_PREFIX:
            newkey = ''.join(['$',key[1:]])
        return newkey
    ## END DEF 

    ## DEF 
    @staticmethod
    def processPeriodReplacement(key):
        return string.replace(key, constants.REPLACE_KEY_PERIOD, '.')
    ## END DEF

    ## DEF
    @staticmethod
    def replacePeriod(clause):
        assert not clause is dict 

        new_clause = {}
        for key in clause:
            n_key = ReplayWorker.processPeriodReplacement(key)
            new_clause[n_key] = clause[key]
        return new_clause
    ## END DEF 

    ## DEF
    @staticmethod
    def getWhereClause(whereClause, predicates):
        # get the predicates
        ## FOR
        for attr in whereClause:
            if not attr in predicates:
                raise Exception("Missing predicate for attribute %s" % attr)
            # TOFIX: regrex? 
            if predicates[attr] == constants.PRED_TYPE_RANGE:
                val = whereClause[attr]
                whereClause[attr] = {}
                for op in val:
                    whereClause[attr][ReplayWorker.processDollarReplacement(op)] = val
        ## END FOR 
        return whereClause
    ## END DEF 

    ## DEF
    @staticmethod
    def getFieldClause(fieldClause):
        pass
    ## END DEF

    ## DEF 
    @staticmethod
    def getUpdateClause(updateClause):
        new_clause = {}
        ## FOR
        for key in updateClause:
            n_key = ReplayWorker.processDollarReplacement(key)

            new_clause[n_key] = ReplayWorker.replacePeriod(updateClause[key])
        ## END FOR

        return new_clause
    ## END DEF
    

    def next(self, config):
        try:
            sess = self.replayCursor.next()
        except:
            self.__rewind_cursor__()
            sess = self.replayCursor.next()
        ## TRY

        # Now we are just set with "Replay Queries" since the name is not important(at least Yang thinks so...)
        return "Replay Queries", sess
    ## DEF

    def executeInitImpl(self, config):
        metadata_conn = None
        targetHost = config['replay']['metadata_host']
        try:
            metadata_conn = pymongo.Connection(targetHost)
        except:
            LOG.error("Failed to connect to target MongoDB at %s", targetHost)
            raise
        assert metadata_conn

        self.metadata_db = metadata_conn[config['replay']['new_meta']]
        self.dataset_db = self.conn[config['replay']['new_db']]
        self.collections = [col_name for col_name in self.dataset_db.collection_names()]

        self.__rewind_cursor__()
    ## DEF

    def executeImpl(self, config, txn, sess):
        # TODO: We should check whether they are trying to make a query
        # against a collection that we don't know about so that we can print
        # a nice error message. This is because we know that we should never
        # have an operation that doesn't touch a collection that we don't already
        # know about.
        op_counter = 0
        for op in sess['operations']:
            coll = op['collection']
            if not coll in self.collections:
                msg = "Invalid operation on unexpected collection '%s'" % coll
                if coll.find("$cmd"): # MONGODB system error collection
                    return op_counter
                ## IF
                
            if self.debug:
                LOG.debug("Executing '%s' operation on '%s'" % (op['type'], coll))
            
            # QUERY
            start = time.time()
            if op['type'] == constants.OP_TYPE_QUERY:
                isCount = False

                whereClause = {}
                for content in op["query_content"]:
                    if "#query" in content:
                        whereClause = content["#query"]
                        break
                
                if not whereClause:
                    continue
                
                whereClause = ReplayWorker.getWhereClause(whereClause, op['predicates'])

                # And the second element is the projection
                fieldsClause = None
                if 'query_fields' in op and not op['query_fields'] is None:
                    fieldsClause = op['query_fields']

                if fieldsClause is None:
                    fieldsClause = {}

                # Check whether this is for a count
                if 'count' in op['query_content'][0]:
                    assert "#query" in op['query_content'][0], "OP: " + pformat(op)
                    # Then do a count
                    #whereClause = op['query_content'][0]["#query"]
                    isCount = True
                        
                # Execute!
                # TODO: Need to check the performance difference of find vs find_one
                resultCursor = self.dataset_db[coll].find(whereClause, fieldsClause)
                # Handle sort
                if 'sort' in op['query_content'][0]:
                    sort_content = [(k,pymongo.ASCENDING if v == 1 else pymongo.DESCENDING) for k,v in op['query_content'][0]['sort'].iteritems()]
                    resultCursor.sort(sort_content)

                # Handle limit
                if op["query_limit"] and op["query_limit"] != -1:
                    #try:
                    resultCursor.limit(op["query_limit"])
                    #except:
                        #exit(pformat(op))
                    
                # Handle count
                if isCount:
                    result = resultCursor.count()
                #else:
                    # We have to iterate through the result so that we know that
                    # the cursor has copied all the bytes
                    #result = [r for r in resultCursor]
                # IF
                
                # TODO: For queries that were originally joins, we need a way
                # to save the output of the queries to use as the input for
                # subsequent queries
                
            # UPDATE
            elif op['type'] == constants.OP_TYPE_UPDATE:
                # The first element in the content field is the WHERE clause
                whereClause = op['query_content'][0]
                assert whereClause, "Missing WHERE clause for %s" % op['type']

                print op
                whereClause = ReplayWorker.getWhereClause(whereClause, op['predicates'])

                # The second element has what we're trying to update
                updateClause = op['query_content'][1]
                assert updateClause, "Missing UPDATE clause for %s" % op['type']

                updateClause = ReplayWorker.getUpdateClause(updateClause)
                
                # Let 'er rip!
                # TODO: What about the 'manipulate' or 'safe' flags?
                flags = {}
                if op['update_upsert'] and op['update_upsert'] == 'true':
                    flags['upsert'] = True
                if op['update_multi'] and op['update_multi'] == 'true':
                    flags['multi'] = True
                if len(flags) > 0:
                    result = self.dataset_db[coll].update(whereClause, updateClause, flags)
                else:
                    result = self.dataset_db[coll].update(whereClause, updateClause)
                    

            # INSERT
            elif op['type'] == constants.OP_TYPE_INSERT:
                # Just get the payload and fire it off
                # There's nothing else to really do here
                result = self.dataset_db[coll].insert(op['query_content'])
            # DELETE

            elif op['type'] == constants.OP_TYPE_DELETE:
                # The first element in the content field is the WHERE clause
                whereClause = op['query_content'][0]
                assert whereClause, "Missing WHERE clause for %s" % op['type']
                
                # SAFETY CHECK: Don't let them accidently delete the entire collection
                assert len(whereClause) > 0, "SAFETY CHECK: Empty WHERE clause for %s" % op['type']
                
                whereClause = ReplayWorker.getWhereClause(whereClause, op['predicates'])

                # I'll see you in hell!!
                result = self.dataset_db[coll].remove(whereClause)            
            # UNKNOWN!
            else:
                raise Exception("Unexpected query type: %s" % op['type'])
            end = time.time()
            
            if not 'ori_number' in op:
                op_counter += 1
            else:
                op_counter += op['ori_number']
            if self.debug:
                LOG.debug("%s Result: %s" % (op['type'], pformat(result)))
        ## FOR

        return op_counter
    ## DEF
## CLASS
