import os
import sys

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))
sys.path.append(os.path.join(basedir, "../search"))

from search import Designer
from util import configutil
from message import *

import catalog
import workload
import mongokit

import logging
LOG = logging.getLogger(__name__)

class Worker:
    def __init__(self, config, channel, args):
        self.config = config
        self.channel = channel
        self.args = args
        self.conn = self.establishConnection()
        self.metadata_db = self.conn[self.config.get(configutil.SECT_MONGODB, 'metadata_db')]
        self.dataset_db = self.conn[self.config.get(configutil.SECT_MONGODB, 'dataset_db')]
        
        self.designer = Designer(self.config, self.metadata_db, self.dataset_db, self.channel)
        self.designer.setOptionsFromArguments(self.args)
        
        self.bestLock = None
        
        sendMessage(MSG_INIT_COMPLETED, None, self.channel)
    ## DEF
    
    def execute(self):
        """
            Run LNS/BB search and inform the coordinator once getting a new best design
        """
        sendMessage(MSG_START_EXECUTING, None, self.channel)
        self.designer.search()
    ## DEF
    
    def update(self, data):
        bestCost = data[0]
        bestDesign = data[1]
        
        self.designer.search_method.bestLock.acquire()
        
        self.designer.search_method.bbsearch_method.bestCost = bestCost
        self.designer.search_method.bbsearch_method.bestDesign = bestDesign.copy()
        
        self.designer.search_method.bestLock.release()
    ## DEF
    
    def establishConnection(self):
        ## ----------------------------------------------
        ## Connect to MongoDB
        ## ----------------------------------------------
        hostname = self.config.get(configutil.SECT_MONGODB, 'host')
        port = self.config.getint(configutil.SECT_MONGODB, 'port')
        assert hostname
        assert port
        try:
            conn = mongokit.Connection(host=hostname, port=port)
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
            raise
        ## Register our objects with MongoKit
        conn.register([ catalog.Collection, workload.Session ])

        ## Make sure that the databases that we need are there
        db_names = conn.database_names()
        for key in [ 'dataset_db', ]: # FIXME 'workload_db' ]:
            if not self.config.has_option(configutil.SECT_MONGODB, key):
                raise Exception("Missing the configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
            elif not self.config.get(configutil.SECT_MONGODB, key):
                raise Exception("Empty configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
        ## FOR
        return conn
    ## DEF
    
## CLASS