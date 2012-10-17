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
        self.channel = channel
        self.worker = None
        self.config = None
        self.benchmark = None
        
    def processMessage(self):
        '''Main loop'''
        for item in self.channel:
            msg = getMessage(item)
            LOG.debug("Incoming Message: %s" % getMessageName(msg.header))
            
            # MSG_CMD_INIT
            if msg.header == MSG_CMD_INIT:
                self.config, msgPacket = msg.data
                self.benchmark = self.config['default']['benchmark']
                
                # Initialize Logging
                if 'logfile' in self.config['default'] and self.config['default']['logfile']:
                    logFormat = "%(asctime)s " +\
                                ("WORKER %02d " % self.config['default']['id']) +\
                                "[%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s"
                    logging.basicConfig(level = logging.INFO,
                                        format=logFormat,
                                        datefmt="%m-%d-%Y %H:%M:%S",
                                        filename=self.config['default']['logfile'])
                    LOG.info("Starting new %s remote worker" % self.benchmark.upper())
                if 'debug' in self.config['default'] and self.config['default']['debug']:
                    logging.getLogger().setLevel(logging.DEBUG)
                    
                # Setup Environent
                setupPath(self.benchmark)
                
                # Create worker
                self.worker = self.createWorker()
                self.worker.init(self.config, self.channel, msgPacket)
                
            # MSG_CMD_LOAD
            # Tells the worker thread to start loading the database
            elif msg.header == MSG_CMD_LOAD:
                self.worker.load(self.config, self.channel, msg.data)
            
            # MSG_CMD_STATUS
            # Return the current status of the worker thread
            elif msg.header == MSG_CMD_STATUS:
                self.worker.status(self.config, self.channel, msg.data)
                
            # MSG_CMD_EXECUTE_INIT
            # Tells the worker thread to initialize the benchmark execution
            elif msg.header == MSG_CMD_EXECUTE_INIT:
                self.worker.executeInit(self.config, self.channel, msg.data)
            
            # MSG_CMD_EXECUTE
            # Tells the worker thread to begin executing the benchmark
            # This will only occur once all of the threads complete the
            # EXECUTE_INIT phase.
            elif msg.header == MSG_CMD_EXECUTE:
                self.worker.execute(self.config, self.channel, msg.data)
            
            # MSG_CMD_STOP
            # Tells the worker thread to halt the benchmark
            elif msg.header == MSG_CMD_STOP:
                # TODO
                pass
            
            # MSG_NOOP
            # A empty command that does not return the worker thread to return
            # a response. I forget why we have this...
            elif msg.header == MSG_NOOP:
                pass
            else:
                assert msg.header in MSG_NAME_MAPPING
                LOG.warn("Unexpected message type: %s", MSG_NAME_MAPPING[msg.header])
                return
    ## DEF
            
    def createWorker(self):
        '''Worker factory method'''
        fullName = self.benchmark.title() + "Worker"
        moduleName = 'benchmarks.%s.%s' % (self.benchmark.lower(), fullName.lower())
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

