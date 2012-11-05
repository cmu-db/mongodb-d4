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

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, ".."))

from message import *
from pprint import pprint, pformat
from multi_search_worker import Worker
from ConfigParser import RawConfigParser

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
            LOG.info("Incoming Message: %s" % getMessageName(msg.header))

            # MSG_CMD_INIT
            if msg.header == MSG_CMD_INIT:
                # Create worker
                self.worker = Worker(msg.data[0], self.channel, msg.data[1])
            
            # MSG_CMD_EXECUTE
            # Tells the worker thread to begin the search process
            # This will only occur once all of the threads complete the
            # EXECUTE_INIT phase.
            elif msg.header == MSG_CMD_EXECUTE:
                self.worker.execute()
            
            # MSG_CMD_UPDATE_BEST_COST
            # update the best cost of the current client
            elif msg.header == MSG_CMD_UPDATE_BEST_COST:
                self.worker.update(msg.data)
                
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
## CLASS
