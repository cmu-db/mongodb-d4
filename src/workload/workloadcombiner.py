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
import operator

from util.histogram import Histogram
from util import constants

LOG = logging.getLogger(__name__)

class WorkloadCombiner:
    
    def __init__(self, col_names, workload):
        self.lastDesign = None
        self.col_names = col_names
        self.workload = copy.deepcopy(workload)
        
        # Build indexes from collections to sessions
        self.col_sess_xref = {}
        for col_name in self.col_names:
            self.col_sess_xref[col_name] = []

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
        ## If the design doesn't have any collection embedding, return None
        hasDenormCol = False
        for col_name in design.getCollections():
            if design.isDenormalized(col_name):
                hasDenormCol = True
                break
            ## IF
        ## FOR
            
        if not hasDenormCol:
            return None
        
        collectionsInProperOrder = self.__GetCollectionsInProperOder__(design)

        for col_name in collectionsInProperOrder:
            parent_col = design.getDenormalizationParent(col_name)
            if parent_col:
                self.__combine_queries__(col_name, parent_col)

        self.lastDesign = design.copy()
        
        for sess in self.workload:
            for op in sess['operations']:
                print "collection: ", op['collection']
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
            while cursor > -1: # if cursor is -1, there won't be any embedding happening
                if operations_in_use[cursor]['collection'] == col:
                    combinedQueries.append(operations_in_use.pop(cursor))
                elif operations_in_use[cursor]['collection'] == parent_col and len(combinedQueries) > 0:
                    for query in combinedQueries:
                        operations_in_use[cursor]['query_content'].extend(query['query_content'])
                    combinedQueries = []
                    operations = operations_in_use[:]
                    sess['operations'] = operations[:]
                cursor -= 1
            ## WHILE
            
            # We need to redirect the queries to its new collection
            for op in sess['operations']:
                if op['collection'] == col:
                    op['collection'] = parent_col
                ## IF
            ## FOR
            
            # now this session has operations to the parent collection
            self.col_sess_xref[parent_col].append(sess)
        ## FOR
    # DEF

    # if C -> B and B -> A, we want C to appear first in the __combine_queries__ setup
    def __GetCollectionsInProperOder__(self, design):
        # initialize collection scores dictionary
        collection_scores = {}
        collections = design.getCollections()

        for col in collections:
            collection_scores[col] = 0
        
        for col in collections:
            self.__update_score__(col, design, collection_scores)
        
        sorted_collection_with_Score = sorted(collection_scores.iteritems(), key=operator.itemgetter(1))

        sorted_collection = [x[0] for x in sorted_collection_with_Score]
        print "sorted_collection: ", sorted_collection
        return sorted_collection

    def __update_score__(self, col, design, collection_scores):
        parent_col = design.getDenormalizationParent(col)
        if parent_col:
            collection_scores[parent_col] += 1
            self.__update_score__(parent_col, design, collection_scores)
## CLASS
