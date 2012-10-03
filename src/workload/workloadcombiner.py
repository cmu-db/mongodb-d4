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

import logging
import copy
from pprint import pformat

from util.histogram import Histogram
from util import constants

LOG = logging.getLogger(__name__)

class WorkloadCombiner:
    
    def __init__(self, collections, workload):
        self.lastDesign = None
        self.collections = collections
        self.workload = workload
        
        # We need to make a deepcopy so that we can always get back
        # the original session information
        self.origWorkload = copy.deepcopy(self.workload)
        
        # Build indexes from collections to sessions
        self.col_sess_xref = dict([(col_info["name"], []) for col_info in self.collections])
        for sess in self.workload:
            cols = set()
            for op in sess["operations"]:
                if op["collection"] in self.col_sess_xref:
                    cols.add(op["collection"])
            ## FOR (op)
            for col_name in cols:
                self.col_sess_xref[col_name].append(sess)
        ## FOR (sess)
        
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def process(self, design):
        """
            For a new design, return a modified version of the workload where operations
            are combined with each other based on the denormalization scheme.
        """
        
        
        # If we have a previous design, then get the list of collections
        # that have changed in our new design.
        if not self.lastDesign is None:
            delta = self.design.getDelta(self.lastDesign)
        
            for col_name in delta:
                # Check whether this collection is embedded inside of another
                # TODO: Need to get ancestor
                parent_col = design.getDenormalizationParent(op['collection'])
        
                # TODO
        
        
    ## DEF
    
    def denormalizeSession(self, sess):
        # TODO: The following is old code that used to be in NetworkCostComponent
        previous_op = None
        for op in sess['operations']:
            process = False
            # This is the first op we've seen in this session
            if not previous_op:
                process = True
            # Or this operation's target collection is not embedded
            elif not parent_col:
                process = True
            # Or if either the previous op or this op was not a query
            elif previous_op['type'] <> constants.OP_TYPE_QUERY or op['type'] <> constants.OP_TYPE_QUERY:
                process = True
            # Or if the previous op was
            elif previous_op['collection'] <> parent_col:
                process = True
                # TODO: What if the previous op should be merged with a later op?
                #       We would lose it because we're going to overwrite previous op
            
            if process:
                # TODO: Do something....
                pass
            
            previous_op = op
        ## FOR
        
        #elif self.debug:
            #LOG.debug("SKIP - %s Op #%d on %s [parent=%s / previous=%s]",\
            #op['type'], op['query_id'], op['collection'],\
            #parent_col, (previous_op != None))
            
        
        pass
    ## DEF
    
## CLASS