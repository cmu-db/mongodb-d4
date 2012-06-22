# -*- coding: utf-8 -*-
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
import time
import logging

from .results import *
from .message import *

LOG = logging.getLogger(__name__)

class AbstractCoordinator:
    '''Abstract coordinator.'''
    def __init__(self):
        '''All subclass constructor should not taken any arguments. You can do more initializing work in initialize() method'''
        self.benchmark = None
        self.config = None
        self.load_result = None
        self.total_results = None
        pass
    ## DEF
    
    def benchmarkConfig(self):
        """Returns a dict for the default configuration of the target benchmark"""
        defaultConfig = { }
        for key, description, default in self.benchmarkConfigImpl():
            defaultConfig[key] = (description, default)
        return defaultConfig
    ## DEF
    
    def benchmarkConfigImpl(self):
        """This function needs to be implemented by all sub-classes.
        It should return the items that need to be in your implementation's configuration file.
        Each item in the list is a triplet containing: ( <PARAMETER NAME>, <DESCRIPTION>, <DEFAULT VALUE> )
        """
        raise NotImplementedError("%s does not implement benchmarkConfigImpl" % (self.benchmark))
    ## DEF
    
    def init(self, config, channels):
        '''initialize method. It is recommanded that you send the a CMD_INIT message with the config object to the client side in the method'''
        self.config = config
        self.name = config['default']['name']
        config['default']['debug'] = LOG.isEnabledFor(logging.DEBUG)
        LOG.info("Initializing %s Benchmark Coordinator" % self.name.upper())

        ## Add in the default configuration values for this benchmark
        benchmarkConfig = self.benchmarkConfig()
        for key in benchmarkConfig.keys():
            if not key in self.config[self.name]:
                val = benchmarkConfig[key]
                self.config[self.name][key] = val[1]
                LOG.debug("Setting %s Default Config Parameter: %s" % (self.name.upper(), self.config[self.name][key]))
        ## FOR
        
        ## First initialize our local coordinator
        self.initImpl(self.config, channels)
        
        ## Invoke the workers for this benchmark invocation
        workerId = 0
        for ch in channels:
            workerConfig = dict(self.config.items())
            workerConfig['default']["id"] = workerId
            workerId += 1
            sendMessage(MSG_CMD_INIT, workerConfig, ch)
        ## FOR
            
        ## Block until they all respond with an acknowledgement
        for ch in channels :
            msg = getMessage(ch.receive())
            if msg.header == MSG_INIT_COMPLETED :
                pass
            else:
                pass
        LOG.debug("%s Initialization Completed!" % self.name.upper())
    ## DEF
        
    def loadImpl(self, config, channels):
        '''Benchmark coordinator initialization method'''
        raise NotImplementedError("%s does not implement initImpl" % (self.name.upper()))
        
    def load(self, config, channels):
        ''' distribute loading to a list of channels by sending command message to each of them.\
        You can collect the load time from each channel and returns the total loading time'''
        LOG.info("Loading %s Database" % self.name)
        
        load_start = time.time()
        self.loadImpl(config, channels)
        for ch in channels :
            msg = getMessage(ch.receive())
            if msg.header == MSG_LOAD_COMPLETED :
                pass
            else:
                pass
        self.load_result = time.time() - load_start
        LOG.info("Loading completed in %.2f seconds" % self.load_result)
        
        return None
    ## DEF
        
    def executeImpl(self, config, channels):
        '''Distribute loading to a list of channels by sending command message to each of them'''
        raise NotImplementedError("%s does not implement loadImpl" % (self.name.upper()))
        
    def execute(self, config, channels):
        '''distribute execution to a list of channels by send command message to each of them.\
        You can collect the execution result from each channel'''
        LOG.info("Executing %s Workload" % self.name.upper())
        
        # Tell all the workers to get initialize themselves for a new 
        # round of execution. This will allow them to perform any initialization
        # that is specific to execution
        for ch in channels:
            sendMessage(MSG_CMD_EXECUTE_INIT, None, ch)
        for ch in channels:
            msg = getMessage(ch.receive())
            if msg.header == MSG_INIT_COMPLETED:
                pass
            else:
                msg = "Unexpected return result %s from channel %s" % (getMessageName(msg.header), ch)
                raise Exception(msg)
        ## FOR
            
        # Now tell them to start executing their benchmark
        for ch in channels:
            sendMessage(MSG_CMD_EXECUTE, None, ch)
            
        # Each channel will return back a Result object
        # We will append each one to our global results
        self.total_results = Results()
        for ch in channels:
            msg = getMessage(ch.receive())
            if msg.header == MSG_EXECUTE_COMPLETED:
                r = msg.data
                self.total_results.append(r)
            else:
                LOG.warn("Unexpected return result %s from channel %s" % (getMessageName(msg.header), ch))
                pass
        
        return None
        
    def showResult(self, config, channels):
        print self.total_results.show(self.load_result)
        
       
    def moreProcessing(self, config, channels):
        '''hook'''
        return None
 