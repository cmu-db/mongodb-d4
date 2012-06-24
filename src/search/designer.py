# -*- coding: utf-8 -*-

#from workload import *
import logging

import catalog
from util import *


LOG = logging.getLogger(__name__)

## ==============================================
## Designer
## ==============================================
class Designer():

    def __init__(self, cparser, workload_db, collections):
        self.cparser = cparser
        self.workload_db = workload_db
        self.collections = collections
        
    ## DEF
    
    def generate(self):
        raise NotImplementedError("Missing %s.generate()" % str(self.__init__.im_class))
        
        
    def generateShardingCandidates(self, collection):
        """Generate the list of sharding candidates for the given collection"""
        assert type(collection) == catalog.Collection
        LOG.info("Generating sharding candidates for collection '%s'" % collection["name"])
        
        # Go through the workload and build a summarization of what fields
        # are accessed (and how often)
        found = 0
        field_counters = { }
        for sess in self.workload_db.Session.find({"operations.collection": collection["name"], "operations.type": ["query", "insert"]}):
            print sess
            
            # For now can just count the number of reads / writes per field
            for op in sess["operations"]:
                for field in op["content"]:
                    if not field in op["content"]: op["content"] = { "reads": 0, "writes": 0}
                    if op["type"] == "query":
                        field_counters[field]["reads"] += 1
                    elif op["type"] == "insert":
                        # TODO: Should we ignore _id?
                        field_counters[field]["writes"] += 1
                    else:
                        raise Exception("Unexpected query type '%s'" % op["type"])
                ## FOR
            found += 1
        ## FOR
        if not found:
            LOG.warn("No workload sessions exist for collection '%s'" % collection["name"])
            return
            
        return (fields_counters)
    ## DEF

## CLASS