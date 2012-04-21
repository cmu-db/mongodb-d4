# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
# Yang Lu - http://www.cs.brown.edu/~yanglu/
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
import logging
import traceback
from pprint import pprint, pformat

import drivers
from util import *
from runtime import *
from api.abstractworker import AbstractWorker

LOG = logging.getLogger(__name__)

class TpccWorker(AbstractWorker):
    
    def initImpl(self, config, channel):
        ## Create a handle to the target client driver
        config['system'] = "mongodb"
        realpath = os.path.realpath(__file__)
        basedir = os.path.dirname(realpath)
        if not os.path.exists(realpath):
            cwd = os.getcwd()
            basename = os.path.basename(realpath)
            if os.path.exists(os.path.join(cwd, basename)):
                basedir = cwd
        config['ddl'] = os.path.join(basedir, "tpcc.sql")
        
        ## Create our ScaleParameter stuff that we're going to need
        self._scaleParameters = scaleparameters.makeWithScaleFactor(int(config['warehouses']), float(config['scalefactor']))
        
        driverClass = self.createDriverClass(config['system'])
        assert driverClass != None, "Failed to find '%s' class" % config['system']
        driver = driverClass(config['ddl'])
        assert driver != None, "Failed to create '%s' driver" % config['system']
        driver.loadConfig(config)
        self._driver = driver
    ## DEF
    
    def createDriverClass(self, name):
        full_name = "%sDriver" % name.title()
        mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        assert self._driver != None
        w_ids = msg.data
        loadItems = (1 in w_ids)
        
        try:
            l = loader.Loader(self._driver, self._scaleParameters, w_ids, loadItems)
            self._driver.loadStart()
            l.execute()
            self._driver.loadFinish()   
        except KeyboardInterrupt:
            return -1
        except (Exception, AssertionError), ex:      
            traceback.print_exc(file = sys.stdout)
            raise
    ## DEF
        
    def executeImpl(self, config, channel, msg):
        assert self._driver!=None
        
        config['execute'] = True
        config['reset'] = False
        soe = (config['stop_on_error']=='1')
        
        e = executor.Executor(self._driver, self._scaleParameters, stop_on_error = soe)
        self._driver.executeStart()
        results = e.execute(config['duration'])
        self._driver.executeFinish()
        sendMessage(EXECUTE_COMPLETED, results, channel)
## CLASS