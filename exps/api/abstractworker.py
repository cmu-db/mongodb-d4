# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Yang Lu - http://www.cs.brown.edu/~yanglu/
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
import time
import logging
import traceback
import random
import pymongo
from pprint import pformat

# Designer
import search

# Benchmark API
from .results import *
from .message import *

LOG = logging.getLogger(__name__)

class AbstractWorker:
    '''Abstract Benchmark Worker'''
    def __init__(self):
        ''' All subclass constructor should not take any argument. You can do more initializing work in initializing method '''
        self.config = None
        self.name = None
        self.id = None
        self.stop_on_error = False
        self.lastChannel = None
        pass
    ## DEF
    
    def getWorkerId(self):
        """Return the unique identifier for this worker instance"""
        return self.id
        
    def getWorkerCount(self):
        """Return the total number of workers in this benchmark invocation"""
        return int(self.config['default']['clientprocs'])
    
    def getScaleFactor(self):
        return float(self.config['default']['scalefactor'])
    
    def getBenchmarkName(self):
        return self.name
    
    ## ---------------------------------------------------------------------------
    ## WORKER INIT
    ## ---------------------------------------------------------------------------
    
    def init(self, config, channel, data):    
        '''Worker Initialization. You always must send a INIT_COMPLETED message back'''
        self.lastChannel = channel
        self.config = config
        self.name = config['default']['name']
        self.id = config['default']['id']
        self.stop_on_error = config['default']['stop_on_error']
        self.debug = config['default']['debug']
        
        LOG.info("Initializing %s worker #%d" % (self.name.upper(), self.id))
        if self.debug:
            LOG.setLevel(logging.DEBUG)
            LOG.debug("%s Configuration:\n%s" % (self.name.upper(), pformat(self.config[self.name])))
        
        ## ----------------------------------------------
        ## DATABASE DESIGN
        ## ----------------------------------------------
        self.design = None
        if config['default']['design'] and config['default']['name'] != 'replay':
            if self.debug:
                LOG.debug("Instantiating design from JSON")
            initalD, self.design = search.utilmethods.fromJSON(config['default']['design'])
            if self.debug:
                LOG.debug("Design:\n%s" % self.design)
        
        ## ----------------------------------------------
        ## TARGET CONNECTION
        ## ----------------------------------------------
        self.conn = None
        targetHost = random.choice(config['default']['hosts'])
        if self.debug:
            LOG.debug("Connecting MongoDB database at %s", targetHost)
        try:
            self.conn = pymongo.Connection(targetHost)
        except:
            LOG.error("Failed to connect to target MongoDB at %s", targetHost)
            raise
        assert self.conn
        
        self.initImpl(config, data)
        sendMessage(MSG_INIT_COMPLETED, self.id, channel)
        LOG.info("Finished initializing %s worker #%d" % (self.name.upper(), self.id))
    ## DEF
    
    def initImpl(self, config):
        raise NotImplementedError("%s does not implement initImpl" % (self.name))
        
    ## ---------------------------------------------------------------------------
    ## LOAD
    ## ---------------------------------------------------------------------------
        
    def load(self, config, channel, data):
        '''Perform actual loading. We will always send back LOAD_COMPLETED message'''
        self.lastChannel = channel
        LOG.info("Invoking %s Loader" % self.name)
        self.loadImpl(config, channel, data)
        sendMessage(MSG_LOAD_COMPLETED, self.getWorkerId(), channel)
        pass
    ## DEF
    
    def loadStatusUpdate(self, completed):
        assert self.lastChannel
        data = (self.getWorkerId(), completed)
        sendMessage(MSG_LOAD_STATUS, data, self.lastChannel)
    ## DEF
    
    def loadImpl(self, config, channel, data):
        raise NotImplementedError("%s does not implement loadImpl" % (self.name))
        
    ## ---------------------------------------------------------------------------
    ## GET DATABASE STATUS
    ## ---------------------------------------------------------------------------
        
    def status(self, config, channel, data):
        assert self.conn
        
        
    def statusImpl(self, config, channel, data):
        raise NotImplementedError("%s does not implement statusImpl" % (self.name))    
    
        
    ## ---------------------------------------------------------------------------
    ## EXECUTION INITIALIZATION
    ## ---------------------------------------------------------------------------
        
    def executeInit(self, config, channel, data):
        self.lastChannel = channel
        LOG.info("Initializing %s before benchmark execution" % self.name)
        self.executeInitImpl(config)
        sendMessage(MSG_INIT_COMPLETED, self.getWorkerId(), channel)
    ## DEF
    
    def executeInitImpl(self, config):
        raise NotImplementedError("%s does not implement executeInitImpl" % (self.name))

    ## ---------------------------------------------------------------------------
    ## WORKLOAD EXECUTION
    ## ---------------------------------------------------------------------------
        
    def execute(self, config, channel, data):
        ''' Actual execution. You might want to send a EXECUTE_COMPLETED message back with the loading time'''
        self.lastChannel = channel
        config['default']['execute'] = True
        config['default']['reset'] = False
        r = Results()
        assert r
        LOG.info("Executing benchmark for %d seconds" % config['default']['duration'])
        debug = LOG.isEnabledFor(logging.DEBUG)
        self.data = data

        start = time.time()
        LOG.info("Starting warm-up period")
        while (time.time()-start) <= int(config['default']['warmup']):
            #LOG.info(time.time()-start)
            txn, params = self.next(config)
            self.executeImpl(config, txn, params)
            
            
        LOG.info("Turning off warm-up period. Starting to collect benchmark data")    
        start = r.startBenchmark()
        while (time.time() - start) <= int(config['default']['duration']):
            txn, params = self.next(config)
            if params is None:
                break
            txn_id = r.startTransaction(txn)
            if debug: LOG.debug("Executing '%s' transaction" % txn)
            try:
                opCount = self.executeImpl(config, txn, params)
                assert not opCount is None
                r.stopTransaction(txn_id, opCount)
            except KeyboardInterrupt:
                return -1
            except (Exception, AssertionError), ex:
                logging.warn("Failed to execute Transaction '%s': %s" % (txn, ex))
                if debug or self.stop_on_error: traceback.print_exc(file=sys.stdout)
                if self.stop_on_error:
                    raise Exception("WE FAILED --> %s(%s)" % (txn, str(params)))
                r.abortTransaction(txn_id)
                pass
        r.stopBenchmark()
        sendMessage(MSG_EXECUTE_COMPLETED, r, channel)
    ## DEF
        
    def next(self, config):
        raise NotImplementedError("%s does not implement next" % (self.name))
        
    def executeImpl(self, config, txn, params):
        raise NotImplementedError("%s does not implement executeImpl" % (self.name))
        
    def moreProcessing(config, channel, data):
        '''hook'''
        return None
## CLASS

