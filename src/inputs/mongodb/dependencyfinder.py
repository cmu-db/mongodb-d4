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

import logging
import itertools

import workload
from util import Histogram
from util import mathutil
from util import constants

LOG = logging.getLogger(__name__)

class DependencyFinder:
    """Examine the sample data set and find dependency relationships"""
    
    def __init__(self, metadata_db, dataset_db):
        self.metadata_db = metadata_db
        self.dataset_db = dataset_db
        self.comparisons = { }
        self.workload = [ self.metadata_db.Session.fetch() ] 
        
        self.collections = { }
        for col_info in self.metadata_db.Collection.fetch():
            self.collections[col_info["name"]] = col_info
            self.comparisons[col_info["name"]] = KeyComparisons(col_info)
            
        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def process(self):
        for session in self.workload:
            self.processSession(sess)
    ## DEF
    
    def processSession(self, sess):
        for op0, op1 in itertools.combinations(sess["operations"], 2):
            # Skip any pairs that reference the same collection
            if op0["collection"] == op1["collection"]: continue
            
            content0 = workload.getOpContents(op0)
            content1 = workload.getOpContents(op1)
            
            LOG.info(pformat(content0))
            LOG.info("-"*100)
            LOG.info(pformat(content0))
            raise Exception("XXX")
            
            for key0,val0 in content0.iteritems():
                #for key1, val1 in content1.iteritems():
                
                if isinstance(val0, list):
                    pass
                elif isinstance(val0, dict):
                    pass
                else:
                    for key1 in content1.iterkeys():
                        pass
        # FOR
        
    ## DEF
    
    #def compareValues(val0, val1):
        #if isinstance(val0, list):
            #pass
        
## CLASS

class KeyComparisons:
    
    def __init__(self, col_info):
        self.col_info = col_info
        self.comparisons = { }
        for f in col_info["fields"].iterkeys():
            self.comparison[f] = { }
            
    def addComparison(self, f, otherCol, otherKey, match=False):
        assert f in self.comparison
        data = self.comparison.get(otherCol, { })
        if not otherKey in data:
            data[otherKey] = [0, 0]
        data[otherKey][0] += 1
        data[otherKey][1] += 1 if match else 0
        self.comparison[otherCol] = data
    ## DEF
        
## CLASS