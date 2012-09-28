#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo & Yang Lu
# http://www.cs.brown.edu/~pavlo/
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
import re
import glob
import time
import execnet
import logging
from pprint import pprint, pformat

from api.abstractcoordinator import AbstractCoordinator
from api.message import *

import drivers
from runtime import scaleparameters

LOG = logging.getLogger(__name__)

class TpccCoordinator(AbstractCoordinator) :
    DEFAULT_CONFIG = [
        ("warehouses", "The number of warehouses to use in the benchmark run", 4), 
        ("denormalize", "If set to true, then the CUSTOMER data will be denormalized into a single document", True),
    ]
    
    def benchmarkConfigImpl(self):
        return self.DEFAULT_CONFIG
    ## DEF
    
    def initImpl(self, config, channels):
        ## Create our ScaleParameter stuff that we're going to need
        num_warehouses = int(config[self.name]['warehouses'])
        self._scaleParameters = scaleparameters.makeWithScaleFactor(num_warehouses, config['default']["scalefactor"])
    ## DEF
    
    def loadImpl(self, config, channels) :
        '''divide loading to several clients'''
        procs = len(channels)
        w_ids = map(lambda x:[], range(procs))
        for w_id in range(self._scaleParameters.starting_warehouse, self._scaleParameters.ending_warehouse+1):
            idx = w_id % procs
            w_ids[idx].append(w_id)
            
        for i in range(len(channels)):
            sendMessage(MSG_CMD_LOAD, w_ids[i], channels[i])
    ## DEF

## CLASS