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

import sys
import logging
import itertools
from pprint import pformat

import workload
from util import Histogram
from util import mathutil
from util import constants

LOG = logging.getLogger(__name__)

class DependencyFinder:
    """Examine the sample data set and find dependency relationships"""
    
    def __init__(self, metadata_db, dataset_db):
        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.metadata_db = metadata_db
        self.dataset_db = dataset_db
        self.comparisons = { }
        self.workload = [ sess for sess in self.metadata_db.Session.fetch() ] 
        assert len(self.workload) > 0
        
        self.collections = { }
        for col_info in self.metadata_db.Collection.fetch():
            self.collections[col_info["name"]] = col_info
            self.comparisons[col_info["name"]] = KeyComparisons()
            
    ## DEF
    
    def process(self):
        LOG.info("# of Sessions: %d", len(self.workload))
        for sess in self.workload:
            self.processSession(sess)
            
        for col_name,col_info in self.collections.iteritems():
            # Set of <ThisCollectionKey, OtherCollectionName, OtherCollectionKey>
            matches = self.comparisons[col_name].getMatches()
            if matches:
                for key0, parent_col, parent_key in matches:
                    splits = key0.split('.')
                    if len(splits) > 1:
                        continue
                    if not 'parent_candidates' in col_info['fields'][key0]:
                        col_info['fields'][key0]['parent_candidates'] = [ ]
                    col_info['fields'][key0]['parent_candidates'].append((parent_col, parent_key))
                ## FOR
                col_info.save()
            ## IF
        ## FOR
    ## DEF
    
    def processSession(self, sess):
        #if len(sess["operations"]) > 1:
            #LOG.info(pformat(sess["operations"]))
            #LOG.info("-"*100)
            
        for op0, op1 in itertools.combinations(sess["operations"], 2):
            # Skip any pairs that reference the same collection
            if op0["collection"] == op1["collection"]: continue
            if op0["query_id"] == op1["query_id"]: continue
            if op0["collection"].endswith("$cmd"): continue
            if op1["collection"].endswith("$cmd"): continue
            
            content0 = workload.getOpContents(op0)
            if len(content0) == 0: continue
            content1 = workload.getOpContents(op1)
            if len(content1) == 0: continue

            values0 = self.extractValues(content0[0])
            #LOG.info("CONTENT0:\n" + pformat(op0))
            #LOG.info("VALUES0:\n" + pformat(values0))
            #LOG.info("="*120)
            #LOG.info("="*120)
            
            values1 = self.extractValues(content1[0])
            #LOG.info("CONTENT1:\n" + pformat(op1))
            #LOG.info("VALUES1:\n" + pformat(values1))
            
            assert op0["collection"] in self.comparisons, op0["collection"]+"-->"+str(self.comparisons.keys())
            compare = self.comparisons[op0["collection"]]
            for key0, key1 in itertools.product(values0.keys(), values1.keys()):
                compare.addComparison(key0, values0[key0], op1["collection"], key1, values1[key1])
        # FOR
    ## DEF
    
    def extractValues(self, content, parent="", extracted=None):
        if extracted is None: extracted = dict()
        
        def _getListValues(value_list, parent, extracted):
            for v in value_list:
                if isinstance(v, dict):
                    self.extractValues(v, parent, extracted)
                elif isinstance(v, list):
                    _getListValues(v, parent, extracted)
                else:
                    if not parent in extracted: extracted[parent] = [ ]
                    extracted[parent].append(v)
            ## FOR
            return
        ## DEF
        
        for k,v in content.iteritems():
            # Special Handling for commands
            if k.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX):
                next_parent = parent
                if k == "#in" or k == "#nin":
                    _getListValues(v, parent, extracted)
                    continue
                elif k == "#date":
                    pass
                elif k == "#oid":
                    pass
                # Things to ignore
                elif k in ['#not', '#options', '#regex', '#lte', '#lt', '#gte', '#gt', '#or', '#ne', '#exists']:
                    continue
                else:
                    LOG.info(pformat(content))
                    raise Exception("!!!!!!!!!!!!")
            else:
                next_parent = parent + ("."+k if parent else k)
                
            if isinstance(v, dict):
                self.extractValues(v, next_parent, extracted)
            elif isinstance(v, list):
                _getListValues(v, next_parent, extracted)
            else:
                if not next_parent in extracted: extracted[next_parent] = [ ]
                extracted[next_parent].append(v)
        ## FOR
        return (extracted)
    ## DEF
        
## CLASS

class KeyComparisons:
    
    def __init__(self):
        self.comparisons = { }
            
    def addComparison(self, key0, values0, col1, key1, values1):
        data = self.comparisons.get(key0, { })
        if not col1 in data:
            data[col1] = { }
        if not key1 in data[col1]:
            data[col1][key1] = {"matches": 0, "total": 0}
        values1 = set(values1)
        for v0 in values0:
            data[col1][key1]["total"] += 1
            if v0 in values1:
                data[col1][key1]["matches"] += 1
        ## FOR
        self.comparisons[key0] = data
    ## DEF
    
    def getMatches(self):
        matches = set()
        for key0, data in self.comparisons.iteritems():
            for col1, inner in data.iteritems():
                for key1,innerMatches in inner.iteritems():
                    if innerMatches["matches"] > 0:
                        matches.add((key0, col1, key1))
            ## FOR
        ## FOR
        return matches
    ## DEF
        
## CLASS