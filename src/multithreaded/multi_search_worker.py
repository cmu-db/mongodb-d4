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
    def __init__(self, config, args, channel, worker_id):
        self.config = config
        self.channel = channel
        self.args = args
        self.designer = None
        self.bestLock = None
        self.worker_id = worker_id
        
        sendMessage(MSG_INIT_COMPLETED, self.worker_id, self.channel)
    ## DEF
    
    def load(self):
        """
            Load data from mongodb
        """
        self.designer = self.establishConnection(self.config, self.args, self.channel)
        initialCost, initialDesign = self.designer.load()
        sendMessage(MSG_INITIAL_DESIGN, (initialCost, initialDesign, self.worker_id), self.channel)
    ## DEF
    
    def execute(self, initialCost, initialDesign):
        """
            Run LNS/BB search and inform the coordinator once getting a new best design
        """
        sendMessage(MSG_START_SEARCHING, self.worker_id, self.channel)
        self.designer.search(initialCost, initialDesign, self.worker_id)
        sendMessage(MSG_EXECUTE_COMPLETED, self.worker_id, self.channel)
    ## DEF
    
    def update(self, data):
        bestCost = data[0]
        bestDesign = data[1]
        
        self.designer.search_method.bbsearch_method.updateBest(bestCost, bestDesign)
        sendMessage(MSG_FINISHED_UPDATE, self.worker_id, self.channel)
    ## DEF
    
    def establishConnection(self, config, args, channel):
        ## ----------------------------------------------
        ## Connect to MongoDB
        ## ----------------------------------------------
        hostname = config.get(configutil.SECT_MONGODB, 'host')
        port = config.getint(configutil.SECT_MONGODB, 'port')
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
            if not config.has_option(configutil.SECT_MONGODB, key):
                raise Exception("Missing the configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
            elif not config.get(configutil.SECT_MONGODB, key):
                raise Exception("Empty configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
        ## FOR
        
        metadata_db = conn[config.get(configutil.SECT_MONGODB, 'metadata_db')]
        dataset_db = conn[config.get(configutil.SECT_MONGODB, 'dataset_db')]
        
        designer = Designer(config, metadata_db, dataset_db, channel)
        designer.setOptionsFromArguments(args)
        
        return designer
    ## DEF
    
## CLASS