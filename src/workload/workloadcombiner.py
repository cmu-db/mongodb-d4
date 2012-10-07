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
        self.col_sess_xref = {}
        for col_info in self.collections.itervalues():
            self.col_sess_xref[col_info["name"]] = []

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
        for col_name in design.getCollections():
            parent_col = design.getDenormalizationParent(col_name)
            if parent_col:
                self.__combine_queries__(col_name, parent_col)

        self.lastDesign = design.copy()

        return self.workload
    ## DEF
    
    # If we want to embed queries accessing collection B to queries accessing collection A
    # We just remove all the queries that
    def __combine_queries__(self, col, parent_col):
        # Get the sessions that contain queries to this collection
        sessions = self.col_sess_xref[col]
        for sess in sessions:
            operations = sess['operations']
            operations_in_use = operations[:]
            cursor = len(operations)  - 1
            combinedQueries = []
            while cursor > 0: # if cursor is 0, there won't be any embedding happening
                for i in xrange(len(operations) - 1, - 1, -1):
                    if operations_in_use[i]['collection'] == col:
                        combinedQueries.append(operations_in_use.pop(i))
                    elif operations_in_use[i]['collection'] == parent_col and len(operations_in_use) < len(operations):
                        operations_in_use[i]['embedded_collection'] = combinedQueries[:]
                        combinedQueries = []
                        operations = operations_in_use[:]
                        sess['operations'] = operations[:]
                        cursor = i
                        break
    # DEF
    
## CLASS
