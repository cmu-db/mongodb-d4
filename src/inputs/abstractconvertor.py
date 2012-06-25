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

from util import constants

## ==============================================
## Abstract Convertor
## ==============================================
class AbstractConvertor():
    
    def __init__(self, metadata_db, dataset_db):
        self.metadata_db = metadata_db
        self.dataset_db = dataset_db

        # The WORKLOAD collection is where we stores sessions+operations
        self.workload_col = self.metadata_db[constants.COLLECTION_WORKLOAD]

        # The SCHEMA collection is where we will store the metadata information that
        # we will derive from the RECREATED database
        self.schema_col = self.metadata_db[constants.COLLECTION_SCHEMA]

        self.stop_on_error = False
        self.limit = None
        self.skip = None
        self.clean = None
        
        self.no_load = False
        self.no_reconstruct = False
        self.no_sessionizer = False
    ## DEF
        
    def process(self):
        raise NotImplementedError("Unimplemented %s.process()" % (self.__init__.im_class))
    ## DEF

## CLASS


    
