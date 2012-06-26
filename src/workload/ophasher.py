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
import logging
from pprint import pformat

from util.histogram import Histogram
from util import constants

LOG = logging.getLogger(__name__)

class OpHasher:
    
    def __init__(self):
        self.histogram = Histogram()
        pass
    ## DEF
    
    def hash(self, op):
        """Compute a deterministic signature for the given operation based on its keys"""
        
        fields = None
        updateFields = None
        
        # QUERY
        if op["type"] == constants.OP_TYPE_QUERY:
            # The query field has our where clause
            if not "#query" in op["query_content"][0]:
                msg = "Missing query field in query_content for operation #%d" % op["query_id"]
                raise Exception(msg)

            fields = op["query_content"][0][constants.REPLACE_KEY_DOLLAR_PREFIX + "query"]

        # UPDATE
        elif op["type"] == constants.OP_TYPE_UPDATE:
            # The first element in the content field is the WHERE clause
            fields = op["query_content"][0]
            
            # We use a separate field for the updated columns so that 
            updateFields = op['query_content'][1]

        # INSERT
        elif op["type"] == constants.OP_TYPE_INSERT:
            # They could be inserting more than one document here,
            # which all may have different fields...
            # So we will need to build a histogram for which keys are referenced
            # and use the onese that appear the most
            # XXX: We'll only consider keys in the first-level
            h = Histogram()
            for doc in op["query_content"]:
                assert type(doc) == dict, "Unexpected insert value:\n%s" % pformat(doc)
                for k in doc.keys():
                    h.put(k)
            ## FOR
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("Insert '%s' Keys Histogram:\n%s" % (op["collection"], h))
            maxKeys = h.getMaxCountKeys()
            assert len(maxKeys) > 0, \
                "No keys were found in %d insert documents?" % len(op["query_content"])
            
            fields = { }
            for doc in op["query_content"]:
                for k, v in doc.iteritems():
                    if k in maxKeys:
                        fields[k] = v
                ## FOR
            ## FOR
            
        # DELETE
        elif op["type"] == constants.OP_TYPE_DELETE:
            # The first element in the content field is the WHERE clause
            fields = op["query_content"][0]
        # UNKNOWN!
        else:
            raise Exception("Unexpected query type: %s" % op["type"])
        
        # Extract the list of fields that are used
        try:
            fieldsHash = self.computeFieldsHash(fields)
        except:
            LOG.error("Unexpected error when processing operation %d [fields=%s]" % (op["query_id"], str(fields)))
            raise
        updateHash = self.computeFieldsHash(updateFields) if updateFields else None
        
        t = (op["collection"], op["type"], fieldsHash, updateHash)
        h = hash(t)
        LOG.debug("%s %s => HASH:%d" % (fields, t, h))
        self.histogram.put(h)
        return h
    ## DEF
    
    def computeFieldsHash(self, fields):
        f = [ ]
        if fields:
            for k, v in fields.iteritems():
                if type(v) == dict:
                    f.append(self.computeFieldsHash(v))
                else:
                    f.append(k)
            ## FOR
        ## IF
        return hash(tuple(sorted(f)))
    ## DEF
    
## CLASS