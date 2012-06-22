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
import string
import random
import logging
from pprint import pprint, pformat

# Designer
from util import constants

# Benchmark
from api.abstractcoordinator import AbstractCoordinator
from api.message import *

LOG = logging.getLogger(__name__)

class ReplayCoordinator(AbstractCoordinator):
    DEFAULT_CONFIG = {
        "host":     ("The hostname of database with the workload to replay", "localhost"),
        "port":     ("The port number of the workload database", 27017),
        "workloaddb":   ("Name of the database with the workload", "metadata"),
        "workloadcollection": ("Name of the workload collection containing the sessions that we want to replay", constants.COLLECTION_WORKLOAD),
        "datadb":   ("Name of the database with the original database", "dataset"),
        "ignorecollections": ("If set to true, the worker will ignore operations that attempt to access an unexpected collection", False),
    }
    
    def benchmarkConfigImpl(self):
        return self.DEFAULT_CONFIG
    ## DEF
    
    def initImpl(self, config, channels):
        # Nothing to do over here...
        pass
    ## DEF
    
    def loadImpl(self, config, channels):
        # TODO: Figure out how we are going to convert the original
        # database to whatever design that they want us to have
        for ch in channels:
            sendMessage(MSG_CMD_LOAD, None, ch)
    ## DEF

## CLASS