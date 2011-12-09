# -*- coding: utf-8 -*-

import sys
import json
import logging
from common import *

## ==============================================
## CostModel
## ==============================================
class CostModel(object):
    
    def __init__(self, cparser):
        assert self != None
        self.cparser = cparser
    
    def estimateCost(self, design, workload):
        assert self != None
        # TODO
    
    def networkCost(self, design, sess):
        assert self != None
        
        # Look at each operation and figure out how network messages we're
        # going to need to send to pull in this query
        # TODO: Do we need to consider both reads and writes?
        messages = 0
        for op in sess["operations"]:
            assert op["collection"] in design["collections"], "Unexpected collection '%s'" % op["collection"]
            catalog_coll = design["collections"]
            
            # Check whether this op accesses the documents using the
            # the collection's sharding key
            # TODO: Need to cache the list of fields so that this lookup is quick
            if catalog_coll["shard_key"] in op["content"].keys():
                # TODO: Great! Now we just need to check whether it's a range query
                messages += 1
            # It's not there, so that means it's going to be a broadcast to all nodes
            else:
                messages += cparser.get(config.SECT_CLUSTER, "nodes")
            ## IF
        # FOR
        return (messages)
    ## DEF
        
    def diskCost(self, design, sess):
        assert self != None
        
    def resourceUtilization(self, workload):
        assert self != None
        # TODO
        
## CLASS
    