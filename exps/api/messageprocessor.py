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
import os
import sys
import logging
from message import *

LOG = logging.getLogger(__name__)

class MessageProcessor:
    ''' Message Processor'''
    def __init__(self, channel):
        self._channel = channel
        self._worker = None
        self._config = None
        self._benchmark = None
        
    def processMessage(self):
        '''Main loop'''
        for item in self._channel:
            msg = getMessage(item)
            LOG.debug("Incoming Message: %s" % getMessageName(msg.header))
            
            # MSG_CMD_INIT
            if msg.header == MSG_CMD_INIT:
                self._config = msg.data
                self._benchmark = self._config['default']['benchmark']
                if 'debug' in self._config['default'] and self._config['default']['debug']:
                    logging.getLogger().setLevel(logging.DEBUG)
                    
                setupPath(self._benchmark)
                self._worker = self.createWorker()
                self._worker.init(self._config, self._channel)
                
            # MSG_CMD_LOAD
            elif msg.header == MSG_CMD_LOAD:
                self._worker.load(self._config, self._channel, msg)
            elif msg.header == MSG_CMD_STATUS:
                self._worker.status(self._config, self._channel, msg)
            elif msg.header == MSG_CMD_EXECUTE_INIT:
                self._worker.executeInit(self._config, self._channel, msg)
            elif msg.header == MSG_CMD_EXECUTE:
                self._worker.execute(self._config, self._channel, msg)
            elif msg.header == MSG_CMD_STOP:
                pass
            elif msg.header == MSG_EMPTY:
                pass
            else:
                assert msg.header in MSG_NAME_MAPPING
                LOG.warn("Unexpected message type: %s", MSG_NAME_MAPPING[msg.header])
                return
    ## DEF
            
    def createWorker(self):
        '''Worker factory method'''
        fullName = self._benchmark.title() + "Worker"
        moduleName = 'benchmarks.%s.%s' % (self._benchmark.lower(), fullName.lower())
        moduleHandle = __import__(moduleName, globals(), locals(), [fullName])
        klass = getattr(moduleHandle, fullName)
        return klass()
## CLASS

## ==============================================
## setupPath
## ==============================================
def setupPath(benchmark):
    realpath = os.path.realpath(__file__)
    basedir = os.path.realpath(os.path.join(os.path.dirname(realpath), ".."))
    if not os.path.exists(realpath):
        cwd = os.getcwd()
        basename = os.path.basename(realpath)
        if os.path.exists(os.path.join(cwd, basename)):
            basedir = cwd
    benchmarkDir = os.path.realpath(os.path.join(basedir, "benchmarks", benchmark))
    if not benchmarkDir in sys.path:
        sys.path.insert(0, benchmarkDir)
## DEF

