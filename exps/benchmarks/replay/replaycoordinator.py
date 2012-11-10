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

import sys
import os
import logging
from pprint import pprint, pformat

from message import *

LOG = logging.getLogger(__name__)

basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "../../../libs"))
sys.path.append(os.path.join(basedir, "../../../src"))
sys.path.append(os.path.join(basedir, "../../tools"))

import pymongo
import mongokit

from util import configutil
import catalog
import workload
import Queue

from denormalizer import Denormalizer
from design_deserializer import Deserializer

class ReplayCoordinator:
    def __init__(self):
        self.metadata_db = None
        self.dataset_db = None
        self.channels = None
        self.config = None
    ## DEF
    
    def init(self, config, channels):
        self.channels = channels
        self.config = config
        
        start = time.time()
        mch = execnet.MultiChannel(self.channels)
        self.queue = mch.make_receive_queue()
    
        # Tell every client to start
        self.send2All(MSG_CMD_INIT, config)
        
        # Count down until all clients are initialized
        num_clients = len(self.channels)
        while True:
            try:
                chan, res = self.queue.get(timeout=60)
                assert getMessage(res).header == MSG_INIT_COMPLETED
                num_clients -= 1
                if num_clients == 0:
                    break
            except Exception:
                LOG.info("WAITING, clients left: %s, elapsed time: %s", num_clients, time.time() - start)
                pass
        ## WHILE
        
        LOG.info("All clients are initialized")
        LOG.info("Loading time: %s", time.time() - start)
    ## DEF
    
    def prepare(self):
        # STEP 0: establish connection to mongodb
        self.connect2mongo()
        
        # STEP 1: Reconstruct database and workload based on the given design
        design = self.getDesign()
        d = Denormalizer(self.metadata_db, self.dataset_db, design)
        d.process()
        
        # STEP 2: Send load command to all workers
        LOG.info("Sending out load database commands")
        self.send2All(MSG_CMD_LOAD_DB, None)
        
        num_of_response = 0
        
        while True:
            try:
                chan, res = self.queue.get(timeout=60)
                msg = getMessage(res)
                if msg.header == MSG_INITIAL_DESIGN:
                    num_of_response += 1
                    LOG.info("Got one initial design")
                    
                    if num_of_response == len(self.channels):
                        LOG.info("All workers are ready to GO")
                        break
                    ## IF
                else:
                    LOG.info("INVALID command %s", msg.header)
                    LOG.info("invalid data\n%s", msg.data)
                    exit("CUPCAKE")
            except Queue.Empty:
                LOG.info("WAITING, Got %d responses", num_of_response)
        ## WHILE
    ## DEF
    
    def execute(self):
        """
            send messages to channels to tell them to start running
            queries against the database
        """ 
        start = time.time()
        
        self.sendExecuteCommand()
        
        end = time.time()
        LOG.info("All the clients finished executing")
        LOG.info("Time elapsed: %s", end - start)
    ## DEF
    
    def getDesign(self):
        design_path = self.config.get(configutil.SECT_REPLAY, 'design')
        
        deserializer = Deserializer()
        deserializer.loadDesignFile(design_path)
        
        design = deserializer.Deserialize()
        LOG.info("current design \n%s" % design)
        return design
    ## DEF
    
    def send2All(self, cmd, message):
        for channel in self.channels:
            sendMessage(cmd, message, channel)
        ## FOR
    ## DEF
    
    def sendExecuteCommand(self):
        self.send2All(MSG_CMD_EXECUTE, None)
        
        running_clients = len(self.channels)
        started_process = 0
        
        while True:
            try:
                chan, res = self.queue.get(timeout=10)
                msg = getMessage(res)
                
                if msg.header == MSG_START_NOTICE:
                    LOG.info("One process started executing, we are good :)")
                    started_process += 1
                    if started_process == len(self.channels):
                        LOG.info("Perfect! All the processes have started executing")
                ## IF
                elif msg.header == MSG_EXECUTE_COMPLETED:
                    running_clients -= 1
                    LOG.info("One process has terminated, there are %d left.", running_clients)
                    if running_clients == 0:
                        break
                ## ELIF
                else:
                    LOG.info("Invalid message %s", msg)
                    exit("CUPCAKE")
                ## ELSE
            except Queue.Empty:
                LOG.info("WAITING, clients left: %s", running_clients)
        ## WHILE
    ## DEF
    
    def connect2mongo(self):
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

        self.metadata_db = conn[self.config.get(configutil.SECT_MONGODB, 'metadata_db')]
        self.dataset_db = conn[self.config.get(configutil.SECT_MONGODB, 'dataset_db')]
    ## DEF
## CLASS