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
        
        
    def generateShardingCandidates(self, collection):
        """Generate the list of sharding candidates for the given collection"""
        assert type(collection) == catalog.Collection
        LOG.info("Generating sharding candidates for collection '%s'" % collection["name"])
        
        # Go through the workload and build a summarization of what fields
        # are accessed (and how often)
        for session in self.workload_db.Session.find({"operations.collection": collection["name"]}):
            print session
        ## FOR
        
    ## DEF

## CLASS