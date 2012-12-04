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

import os
import sys
import logging
from pprint import pformat
import time
import copy

# Third-Party Dependencies
basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "../../../libs"))

# mongodb-d4
sys.path.append(os.path.join(basedir, "../../../src"))
sys.path.append(os.path.join(basedir, "../../tools"))

import catalog
import workload
from catalog import Collection
from workload import Session
from util import Histogram
from util import constants
from workload import WorkloadCombiner
import copy

LOG = logging.getLogger(__name__)

class Denormalizer:
    """MongoDB Database Denormalizer"""

    def __init__(self, metadata_db, dataset_db, design):
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.metadata_db = metadata_db
        self.dataset_db = dataset_db
        
        self.design = design
        self.src2des = { } # Dict that map source collection to destination collection
        ## DEF

    def process(self):
        """Iterates through all operations of all sessions and denormalizs the dataset..."""
        
        # STEP 0: Check the whole design and see if there are collections that have denorm field
        self.loadDesign()
        
        # STEP 1: Re-construct the operations in the workload
        new_workload = self.combineOperations()
        
        # STEP 1.5: Update the metadata_db in the database
        if new_workload:
            self.updateMetadata(new_workload)
        ## IF
        
        # STEP 2: Remove the documents from the source collection and put them into the destination collection
        self.migrateDocuments()
    ## DEF

    def updateMetadata(self, workload):
        # we first remove all the sessions in the metadata_db: This sounds crazy but...yeah...
        self.metadata_db.sessions.remove()
        
        # After that save the new sessions
        for sess in workload:
            self.metadata_db.sessions.save(sess)
        ## FOR
    ## DEF
    
    def migrateDocuments(self):
        if len(self.src2des) == 0:
            return None
        ## IF
        
        # Put the documents from source collection into destination collection
        for src_col, des_col in self.src2des.iteritems():
            for doc in self.dataset_db[src_col].find():
                try:
                    self.dataset_db[des_col].save(doc)
                except:
                    msg = "Unexpected error when processing '%s' data fields" % col_name
                    msg += "\n" + pformat(doc)
                    LOG.error(msg)
                    raise
                ## TRY
            ## FOR
        ## FOR
        
        # Remove all documents from the source collection, we can just delete the collection
        for col_name in self.src2des.iterkeys():
            self.dataset_db[col_name].drop()
        ## FOR
        
    ## DEF
    
    def loadDesign(self): 
        for col_name in self.design.getCollections():
            if self.design.isDenormalized(col_name):
                self.src2des[col_name] = self.design.getDenormalizationParent(col_name)
            ## IF
        ## FOR
    ## DEF

    def combineOperations(self):
        if len(self.src2des) == 0:
            return None
        ## IF
        LOG.info("Denormalizing Database")
        col_names = [ x for x in self.dataset_db.collection_names()]
        workload = [x for x in self.metadata_db.sessions.find()]
        combiner = WorkloadCombiner(col_names, workload)
        new_workload = combiner.process(self.design)
        assert new_workload
        return new_workload
    ## FOR
## CLASS