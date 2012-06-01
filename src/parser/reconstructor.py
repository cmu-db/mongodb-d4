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
sys.path.append("../workload")
sys.path.append("../sanitizer")
import anonymize
from traces import Session

LOG = logging.getLogger(__name__)

## ==============================================
## DEFAULT VALUES
## ==============================================

# Original Code: Emanuel Buzek
class Reconstructor:
    """Mongosniff Database Reconstructor"""
    
    def __init__(self, workload_col, recreated_db):
        self.workload_col = workload_col
        self.recreated_db = recreated_db

        pass
    ## DEF

    def process(self):
        """Iterates through all operations of all sessions and recreates the dataset..."""
        cnt = self.workload_col.find().count()
        LOG.info("Found %d sessions in the workload collection. Processing... ", cnt)
        for session in self.workload_col.find():
            for op in session["operations"]:
                if op["type"] == parser.OP_TYPE_QUERY:
                    processQuery(op)
                elif op["type"] == parser.OP_TYPE_DELETE:
                    processDelete(op)
                elif op["type"] == parser.OP_TYPE_UPDATE:
                    processUpdate(op)
                elif op["type"] in [parser.OP_TYPE_INSERT, parser.OP_TYPE_ISERT]:
                    processInsert(op)
                else:
                    LOG.warn("Unknown operation type: %s", op["type"])
        LOG.info("Done.")
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

    def processUpdate(op):
        payload = op["query_content"]
        col = op["collection"]
        upsert = op["update_upsert"]
        multi = op["update_multi"]
        LOG.info("Updating collection %s. Upsert: %s, Multi: %s", col,str(upsert), str(multi))
        assert len(payload) == 2, 
            "Update operation payload is expected to have exactly 2 entries."
        self.recreated_db[col].update(payload[0], payload[1], upsert, multi)
    ## DEF

    def processQuery(op):
        if op["query_aggregate"] == 1:
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