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

import random
import itertools
import logging

# mongodb-d4
from design import Design
import workload
from util import Histogram, configutil, constants
from abstractdesigner import AbstractDesigner

LOG = logging.getLogger(__name__)

# Constants
INITIAL_INDEX_MEMORY_ALLOCATION = 0.5

## ==============================================
## InitialDesigner
## ==============================================
class InitialDesigner(AbstractDesigner):
    
    def __init__(self, collections, workload, config):
        AbstractDesigner.__init__(self, collections, workload, config)
        self.address_size = constants.DEFAULT_ADDRESS_SIZE
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
    
    def generate(self):
        if self.debug:
            LOG.debug("Computing initial design")
        design = Design()
        
        # STEP 1
        # Generate a histogram of the sets of keys that are used together
        col_keys = self.generateCollectionHistograms()
        map(design.addCollection, col_keys.iterkeys())

        # STEP 2
        # Select the sharding key for each collection as the set of keys
        # that are occur most often
        self.__selectShardingKeys__(design, col_keys)
        
        # STEP 3 
        # Iterate through the collections and keep adding indexes until
        # we exceed our initial design memory allocation
        #total_memory = self.config.getint(configutil.SECT_CLUSTER, "node_memory") * INITIAL_INDEX_MEMORY_ALLOCATION
        total_memory = 512
        assert total_memory > 0
        self.__selectIndexKeys__(design, col_keys, total_memory)
            
        return design
    ## DEF
    
    def generateCollectionHistograms(self):
        col_keys = dict([(col_name, Histogram()) for col_name in self.collections])
        for sess in self.workload:
            for op in sess["operations"]:
                if op["collection"].find("$cmd") != -1:
                    continue
                assert op["collection"] in col_keys, "Missing: " + op["collection"]
                fields = workload.getReferencedFields(op)
                h = col_keys[op["collection"]]
                for i in xrange(1, len(fields)+1):
                    map(h.put, itertools.combinations(fields, i))
            ## FOR (op)
        ## FOR (sess)
        return (col_keys)
    ## DEF
    
    def __selectShardingKeys__(self, design, col_keys):
        for col_name, h in col_keys.iteritems():
            max_keys = h.getMaxCountKeys()
            if self.debug:
                LOG.debug("Sharding Key Candidates %s => %s", col_name, max_keys)
            design.addShardKey(col_name, random.choice(max_keys))
        ## FOR
    ## DEF
    
    def __selectIndexKeys__(self, design, col_keys, total_memory):
        while len(col_keys) > 0:
            to_remove = [ ]
            for col_name, h in col_keys.iteritems():
                # Iterate through all the possible keys for this collection
                for index_keys in sorted(h.iterkeys(), key=lambda k: h[k]):
                    # TODO: Estimate the amount of memory used by this index
                    index_memory = self.__getIndexSize__(self.collections[col_name], index_keys)
                    
                    # We always want to remove index_keys from the histogram
                    # even if there isn't enough memory, because we know that 
                    # we will never be able to add it again
                    del h[index_keys]
                    
                    # If we still have enough memory, then we can add it
                    # We will then break out of the loop and examine the next
                    # collection
                    if index_memory < total_memory:
                        if self.debug:
                            LOG.debug("Adding index %s for %s [memory=%d]", index_keys, col_name, index_memory)
                        design.addIndex(col_name, index_keys)
                        total_memory -= index_memory
                        break
                ## FOR
                
                # Mark this collection to be removed if it doesn't
                # have anymore index keys left
                if len(h) == 0:
                    if self.debug:
                        LOG.debug("Finished evaluating all indexes for %s", col_name)
                    to_remove.append(col_name)
            ## FOR
            map(col_keys.pop, to_remove)
        ## WHILE
    ## DEF
    def __getIndexSize__(self, col_info, indexKeys):
        """Estimate the amount of memory required by the indexes of a given design"""
        # TODO: This should be precomputed ahead of time. No need to do this
        #       over and over again.
        index_size = 0
        for f_name in indexKeys:
            f = col_info.getField(f_name)
            assert f, "Invalid index key '%s.%s'" % (col_info['name'], f_name)
            index_size += f['avg_size']
        index_size += self.address_size
        if self.debug: 
            LOG.debug("%s Index %s Memory: %d bytes", col_info['name'], repr(indexKeys), index_size)
        return index_size
## CLASS