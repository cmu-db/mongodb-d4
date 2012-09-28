# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
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

from datetime import datetime

import constants

## ==============================================
## AbstractDriver
## ==============================================
class AbstractDriver(object):
    def __init__(self, name, ddl):
        self.name = name
        self.driver_name = "%sDriver" % self.name.title()
        self.ddl = ddl
        
    def __str__(self):
        return self.driver_name
       
    def loadStart(self):
        """Optional callback to indicate to the driver that the data loading phase is about to begin."""
        return None
        
    def loadFinish(self):
        """Optional callback to indicate to the driver that the data loading phase is finished."""
        return None

    def loadFinishItem(self):
        """Optional callback to indicate to the driver that the ITEM data has been passed to the driver."""
        return None

    def loadFinishWarehouse(self, w_id):
        """Optional callback to indicate to the driver that the data for the given warehouse is finished."""
        return None
        
    def loadFinishDistrict(self, w_id, d_id):
        """Optional callback to indicate to the driver that the data for the given district is finished."""
        return None
        
    def loadTuples(self, tableName, tuples):
        """Load a list of tuples into the target table"""
        raise NotImplementedError("%s does not implement loadTuples" % (self.driver_name))
        
    def executeStart(self):
        """Optional callback before the execution phase starts"""
        return None
        
    def executeFinish(self):
        """Callback after the execution phase finishes"""
        return None
## CLASS