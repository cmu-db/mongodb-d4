# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Yang Lu
# http://www.cs.brown.edu/~yanglu/
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
import execnet
import logging
from .message import *

LOG = logging.getLogger(__name__)

class AbstractCoordinator:
    '''Abstract coordinator.'''
    def __init__(self):
        '''All subclass constructor should not taken any arguments. You can do more initializing work in initialize() method'''
        self._benchmark = None
        self._config = None
        self._sf = None
        self._load_result = None
        pass
    
    def initialize(self, config, channels):
        '''initialize method. It is recommanded that you send the a CMD_INIT message with the config object to the client side in the method'''
        
        self._config = config
        self._config['name'] = config['benchmark'].upper()
        
        ## Invoke the workers for this benchmark invocation
        for ch in channels :
            sendMessage(MSG_CMD_INIT, self._config, ch)
        ## Block until they all respond with an acknowledgement
        for ch in channels :
            msg = getMessage(ch.receive())
            if msg.header == MSG_INIT_COMPLETED :
                pass
            else:
                pass
        LOG.info("%s Initialization Completed!" % self._config['name'])
    ## DEF
        
    def load(self, config, channels):
        ''' distribute loading to a list of channels by sending command message to each of them.\
        You can collect the load time from each channel and returns the total loading time'''
        
        load_start = time.time()
        self.loadImpl(config, channels)
        for ch in channels :
            msg = getMessage(ch.receive())
            if msg.header == MSG_LOAD_COMPLETED :
                pass
            else:
                pass
        self._load_result = time.time() - load_start
        LOG.info("Loading completed: %s" % self._load_result)
        
        return None
    ## DEF
        
    def loadImpl(self, config, channels):
        '''Distribute loading to a list of channels by sending command message to each of them'''
        return None
        
    def distributeExecution(self, config, channels):
        '''distribute execution to a list of channels by send command message to each of them.\
        You can collect the execution result from each channel'''
        
        ##How to use channel object, see http://codespeak.net/execnet/examples.html
        ##See also ExampleCoordinator.py
        
        return None
        
    def showResult(self, config, channels):
        '''optional result display method'''
        return None
       
    def moreProcessing(self, config, channels):
        '''hook'''
        return None
 