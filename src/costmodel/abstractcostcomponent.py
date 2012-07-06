# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
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

import logging

LOG = logging.getLogger(__name__)

## ==============================================
## Abstract Cost Model Component
## ==============================================
class AbstractCostComponent():
    
    def __init__(self, costModel):
        self.cm = costModel
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF
        
    def getCost(self, design):
        return self.getCostImpl(design)
    ## DEF

    def getCostImpl(self, design):
        raise NotImplementedError("Unimplemented %s.getCostImpl()" % self.__init__.im_class)

    def reset(self):
        """Optional callback for when the cost model needs to reset itself"""
        pass

    def finish(self):
        """Optional callback for when the cost model is finished a round"""
        pass

## CLASS