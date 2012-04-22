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
import execnet
import logging

from .message import *

LOG = logging.getLogger(__name__)

class AbstractWorker:
    '''Abstract Benchmark Worker'''
    def __init__(self):
        ''' All subclass constructor should not take any argument. You can do more initializing work in initializing method '''
        self._config = None
        self._name = None
        self._id = None
        pass
    ## DEF
    
    def getWorkerId():
        """Return the unique identifier for this worker instance"""
        return self._id
    
    def getBenchmarkName():
        return self._name
    
    def init(self, config, channel, msg):
        '''Work Initialization. You always must send a INIT_COMPLETED message back'''
        self._config = config
        self._name = config['name']
        self._id = config["id"]
        
        LOG.info("Initializing %s Worker [clientId=%d]" % (self._name, self._id))
        self.initImpl(config, channel)
        sendMessage(MSG_INIT_COMPLETED, None, channel)
    ## DEF
    
    def initImpl(self, config, channel):
        raise NotImplementedError("%s does not implement initImpl" % (self._name))
        
    def load(self, config, channel, msg):
        '''Perform actual loading. We will always send back LOAD_COMPLETED message'''
        LOG.info("Invoking %s Loader" % config['name'])
        self.loadImpl(config, channel, msg)
        sendMessage(MSG_LOAD_COMPLETED, None, channel)
        pass
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        raise NotImplementedError("%s does not implement loadImpl" % (self._name))
        
    def execute(config, channel, msg):
        ''' Actual execution. You might want to send a EXECUTE_COMPLETED message back with the loading time'''
        config['execute'] = True
        config['reset'] = False
        results = self.executeImpl(config, channel, msg)
        sendMessage(MSG_EXECUTE_COMPLETED, results, channel)
        pass
    ## DEF
        
    def executeImpl(self, config, channel, msg):
        raise NotImplementedError("%s does not implement executeImpl" % (self._name))
        
    def moreProcessing(config, channel, msg):
        '''hook'''
        return None
## CLASS

