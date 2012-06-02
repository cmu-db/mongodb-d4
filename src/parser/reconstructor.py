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
import logging
from pprint import pformat

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# MongoDB-Designer
import anonymize
import parser
from catalog import Collection
from traces import Session
from util.histogram import Histogram
from util import utilmethods

LOG = logging.getLogger(__name__)

## ==============================================
## DEFAULT VALUES
## ==============================================

# Original Code: Emanuel Buzek
class Reconstructor:
    """MongoDB Database Reconstructor"""
    
    def __init__(self, workload_col, recreated_db, schema_col):
        self.workload_col = workload_col
        self.recreated_db = recreated_db
        self.schema_col = schema_col
        
        # Name -> Collection
        self.schema = { }

        pass
    ## DEF

    def process(self):
        """Iterates through all operations of all sessions and recreates the dataset..."""
        cnt = self.workload_col.find().count()
        LOG.info("Found %d sessions in the workload collection. Processing... ", cnt)
        
        self.reconstructDatabase()
        
        self.extractSchema()
        for c in self.collections.values():
            self.schema_col.insert(c)
        ## FOR
        
        LOG.info("Done.")
    ## DEF
    
    def clean(self):
        LOG.warn("Purging existing reconstructed database '%s'" % (self.recreated_db.name))
        db = self.recreated_db.database
        db.connection.drop_database(db.name)
        
        LOG.warn("Purging existing catalog collection '%s.%s'" % (self.schema_col.database.name, self.schema_col.name))
        db = self.schema_col.database
        db.connection.drop_database(db.name)
    ## DEF
    
    def reconstructDatabase(self):
        for session in self.workload_col.find():
            for op in session["operations"]:
                # HACK: Skip any operations with invalid collection names
                #       We will go back later and fix these up
                if op["collection"] == parser.INVALID_COLLECTION_MARKER:
                    continue
                
                if op["type"] == parser.OP_TYPE_QUERY:
                    self.processQuery(op)
                elif op["type"] == parser.OP_TYPE_DELETE:
                    self.processDelete(op)
                elif op["type"] == parser.OP_TYPE_UPDATE:
                    self.processUpdate(op)
                elif op["type"] in [parser.OP_TYPE_INSERT, parser.OP_TYPE_ISERT]:
                    self.processInsert(op)
                else:
                    LOG.warn("Unknown operation type: %s", op["type"])
            ## FOR (operations)
        ## FOR (sessions)
    ## DEF
    
    def extractSchema(self):
        """Iterates through all documents and infers the schema..."""
        cols = self.recreated_db.collection_names()
        LOG.info("Found %d collections. Processing...", len(cols))
        
        for col in cols:
            # Skip system collection
            if col.startswith("system."):
                continue
            c = Collection()
            c['name'] = col
            fields = {}
            for doc in self.recreated_db[col].find():
                self.addKeys(fields, doc, False)
            c['fields'] = fields
            self.collections[col] = c
        ## FOR
    ## DEF

    def fixInvalidCollections(self):
        for session in self.workload_col.find({"operations.collection": parser.INVALID_COLLECTION_MARKER}):
            for op in session["operations"]:
                dirty = False
                if op["collection"] != parser.INVALID_COLLECTION_MARKER:
                    continue
                
                # For each field referenced in the query, build a histogram of 
                # which collections have a field with the same name
                fields = utilmethods.getReferencedFields(op)
                h = Histogram()
                for c in self.collections:
                    for f in c['fields']:
                        if f in fields:
                            h.put(c['name'])
                    ## FOR
                ## FOR
                
                matches = h.getMaxCountKeys()
                if len(matches) == 0:
                    LOG.warn("No matching collection was found for corrupted operation\n%s" % pformat(op))
                    continue
                elif len(matches) > 1:
                    LOG.warn("More than one matching collection was found for corrupted operation %s\n%s" % (matches, pformat(op)))
                    continue
                else:
                    op["collection"] = matches[0]
                    dirty = True
                    LOG.info("Fix corrupted collection in operation\n%s" % pformat(op))
                ## IF
            ## FOR (operations)
            
            if dirty:
                self.workload_col.save(session)
                
        ## FOR (sessions)
        
    ## DEF
    
    def addKeys(self, fields, doc, nested):
        for k in doc.keys():
            fields[k] = {}
            if type(doc[k]) is dict:
                self.addKeys(fields, doc[k], True)
        ## FOR
    ## DEF
    
    def processInsert(self, op):
        payload = op["query_content"]
        col = op["collection"]
        LOG.info("Inserting %d documents into collection %s", len(payload), col)
        for doc in payload:
            LOG.debug("inserting: ", doc)
            self.recreated_db[col].save(doc)
    ## DEF

    def processDelete(self, op):
        payload = op["query_content"]
        col = op["collection"]
        #for doc in payload:
        LOG.info("Deleting documents from collection %s..", col)
        self.recreated_db[col].remove(payload)
    ## DEF

    def processUpdate(self, op):
        payload = op["query_content"]
        col = op["collection"]
        
        LOG.info("Updating Collection '%s' [upsert=%s, multi=%s]" % (col, op["update_upsert"], op["update_multi"]))
        assert len(payload) == 2, 
            "Update operation payload is expected to have exactly 2 entries."
        self.recreated_db[col].update(payload[0], payload[1], op["update_upsert"], op["update_multi"])
    ## DEF

    def processQuery(self, op):
        if op["query_aggregate"]:
            # This is probably AGGREGATE... disregard it
            return
        
        # check if resp_content was set
        if 'resp_content' not in op:
            LOG.warn("Query without response: %s" % str(op))
            return
        
        # The query is irrelevant, we simply add the content of the reply...
        payload = op["resp_content"]
        col = op["collection"]
        LOG.info("Adding %d query results to collection %s", len(payload), col)
        
        #update(old_doc, new_doc, upsert=True, multi=False)
        #this is an upsert operation: insert if not present
        for doc in payload:
            #print "doc:", doc
            self.recreated_db[col].update(doc, doc, True, False)
    ## DEF
    
    
    
    
## CLASS