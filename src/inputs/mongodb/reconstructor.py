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
import logging
from pprint import pformat

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))

# mongodb-d4
sys.path.append(os.path.join(basedir, ".."))
import catalog
import workload
from catalog import Collection
from workload import Session
from util import Histogram
from util import constants

LOG = logging.getLogger(__name__)

# Original Code: Emanuel Buzek
class Reconstructor:
    """MongoDB Database Reconstructor"""
    
    def __init__(self, metadata_db, dataset_db):
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.metadata_db = metadata_db
        self.dataset_db = dataset_db

        self.op_ctr = 0
        self.sess_ctr = 0
        self.skip_ctr = 0
        self.fix_ctr = 0
    ## DEF

    def process(self):
        """Iterates through all operations of all sessions and recreates the dataset..."""
        cnt = self.metadata_db.Session.fetch().count()
        LOG.info("Found %d sessions in the workload collection. Processing... ", cnt)

        # HACK: Skip any operations with invalid collection names
        #       We will go back later and fix these up
        toIgnore = [constants.INVALID_COLLECTION_MARKER] + constants.IGNORED_COLLECTIONS

        LOG.info("Reconstructing dataset from %d Sessions" % self.metadata_db.Session.fetch().count())
        for session in self.metadata_db.Session.fetch():
            self.sess_ctr += 1
            for op in session["operations"]:
                self.op_ctr += 1

                if op["collection"] in toIgnore:
                    self.skip_ctr += 1
                    continue

                if op["type"] == constants.OP_TYPE_QUERY:
                    ret = self.processQuery(op)
                elif op["type"] == constants.OP_TYPE_DELETE:
                    ret = self.processDelete(op)
                elif op["type"] == constants.OP_TYPE_UPDATE:
                    ret = self.processUpdate(op)
                elif op["type"] in [constants.OP_TYPE_INSERT, constants.OP_TYPE_ISERT]:
                    ret = self.processInsert(op)
                else:
                    LOG.warn("Unknown operation type: %s", op["type"])

                if not ret: self.skip_ctr += 1
            ## FOR (operations)
        ## FOR (sessions)
        LOG.info("Processed %d sessions with %d operations [skipped=%d]",
                 self.sess_ctr, self.op_ctr, self.skip_ctr)
    ## DEF
    
    def getSessionCount(self):
        """Return the number of sessions examined"""
        return self.sess_ctr
    ## DEF
    
    def getOpCount(self):
        """Return the number of operations examined"""
        return self.op_ctr
    ## DEF
    
    def getOpSkipCount(self):
        """Return the number of operations that were skipped during processing"""
        return self.skip_ctr
    ## DEF
    
    def getOpFixCount(self):
        """Return the number of operations that were fixed during processing"""
        return self.fix_ctr
    ## DEF
    
    def getCollectionCounts(self):
        """Return a dict of the collections in the recreated database and the number of
           documents that they contain"""
        counts = { }
        for col in self.dataset_db.collection_names():
            if col.split(".")[0] in constants.IGNORED_COLLECTIONS: continue
            counts[col] = self.dataset_db[col].find().count()
        return counts
    ## DEF

    def processInsert(self, op):
        payload = op["query_content"]
        col = op["collection"]
        if self.debug: LOG.debug("Inserting %d documents into collection %s", len(payload), col)
        for doc in payload:
            self.dataset_db[col].save(doc)
        return True
    ## DEF

    def processDelete(self, op):
        payload = op["query_content"]
        col = op["collection"]
        if self.debug: LOG.debug("Deleting documents from collection %s..", col)
        self.dataset_db[col].remove(payload)
        return True
    ## DEF

    def processUpdate(self, op):
        payload = op["query_content"]
        col = op["collection"]

        if self.debug: LOG.debug("Updating Collection '%s' [upsert=%s, multi=%s]" % (col, op["update_upsert"], op["update_multi"]))
        assert len(payload) == 2, \
            "Update operation payload is expected to have exactly 2 entries."
        self.dataset_db[col].update(payload[0], payload[1], op["update_upsert"], op["update_multi"])
        return True
    ## DEF

    def processQuery(self, op):
        col = op["collection"]
        
        # We have to skip aggregates since the response contains computed values
        if op["query_aggregate"]:
            if self.debug: 
                LOG.warn("Skipping operation #%d on '%s' because it is an aggregate function" % (op['query_id'], col))
        
        # Skip anything that doesn't have a response
        elif 'resp_content' not in op:
            if self.debug: 
                LOG.warn("Skipping operation #%d on '%s' because it does not have a response" % (op['query_id'], col))
        
        # The query is irrelevant, we simply add the content of the reply...
        elif len(op["resp_content"]) > 0:
            if self.debug: 
                LOG.debug("Adding %d query results to collection %s", len(op["resp_content"]), col)
        
            # Note that this is an upsert operation: insert if not present
            for doc in op["resp_content"]:
                #print "doc:", doc
                self.dataset_db[col].update(doc, doc, True, False)
            
            return True
        ## IF
        
        return False
    ## DEF

    
## CLASS