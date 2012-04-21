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
from .message import *

LOG = logging.getLogger(__name__)

class AbstractWorker:
    '''Abstract Benchmark Worker'''
    def __init__(self):
        ''' All subclass constructor should not take any argument. You can do more initializing work in initializing method '''
        self._config = None
        pass
    ## DEF
    
    def initialize(self, config, channel):
        '''Work Initialization. You always must send a INIT_COMPLETED message back'''
        LOG.info("Initializing %s Worker" % config['name'])
        self._config = config
        self.initImpl(config, channel)
        sendMessage(MSG_INIT_COMPLETED, None, channel)
    ## DEF
    
    def initImpl(self, config, channel):
        return None
        
    def load(self, config, channel, msg):
        '''Perform actual loading. We will always send back LOAD_COMPLETED message'''
        LOG.info("Invoking %s Loader" % config['name'])
        self.loadImpl(config, channel, msg)
        sendMessage(MSG_LOAD_COMPLETED, None, channel)
        pass
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        return None
        
    def startExecution(config, channel, msg):
        ''' Actual execution. You might want to send a EXECUTE_COMPLETED message back with the loading time'''
        return None
        
    def moreProcessing(config, channel, msg):
        '''hook'''
        return None


