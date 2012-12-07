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
        self.processed_session_ids = set()
        ## DEF

    def process(self):
        """Iterates through all operations of all sessions and denormalizs the dataset..."""
        
        # STEP 0: Check the whole design and see if there are collections that have denorm field
        self.loadDesign()
        
        # STEP 1: Re-construct the operations in the workload
        self.denormalize()
        
        # STEP 2: Remove the documents from the source collection and put them into the destination collection
        self.migrateDocuments()
    ## DEF

    def __query_counter__(self):
        counter = 0
        for sess in self.metadata_db.sessions.find():
            for op in sess['operations']:
                counter += len(op['query_content'])
            ## FOR
        ## FOR
        print "Number of queries: ", counter
    ## FOR

    def denormalize(self):
        if len(self.src2des) == 0:
            return None
        ## IF
        LOG.info("Denormalizing metadata database")
        #self.__query_counter__()
        start_time = time.time()

        col_names = [ x for x in self.dataset_db.collection_names()]
        workload_cursor = self.metadata_db.sessions.find()
        total_sess = workload_cursor.count()
        processed_sess = 0
        error_sess = 0
        left_sess = total_sess
        
        LOG.info("Processing %s sessions", total_sess)
        
        # In order to be RAM friendly, we process limited number of sessions each time
        while left_sess > 0:
            if left_sess >= constants.WORKLOAD_WINDOW_SIZE:
                num_to_be_processed = constants.WORKLOAD_WINDOW_SIZE
            else:
                num_to_be_processed = left_sess
            ## IF
            new_workload, num_error, processed_workload_ids = self.combineOperations(workload_cursor, num_to_be_processed, col_names)
            
            error_sess += num_error
            processed_sess += len(processed_workload_ids)
            if processed_sess % 100000 == 0:
                LOG.info("Processed %s sessions", processed_sess)
            ## IF
            left_sess -= num_to_be_processed

            self.updateMetadata(new_workload, processed_workload_ids)
            # rewind the cursor after we update the database
            workload_cursor = self.metadata_db.sessions.find()
            self.processed_session_ids.update(processed_workload_ids)
        ## WHILE

        LOG.info("Finished metadata denormalization. Total sessions: %s. Error sessions: %s. Processed sessions: %s", total_sess, error_sess, processed_sess)
        LOG.info("Metadata Denormalization takes: %s seconds", time.time() - start_time)
    ## DEF

    def updateMetadata(self, workload, processed_workload_ids):
        # Remove all the processed sessions
        for sess_id in processed_workload_ids:
            self.metadata_db.sessions.remove({"_id" : sess_id})
        ## FOR

        # After that save the new sessions
        for sess in workload:
            self.metadata_db.sessions.save(sess)
        ## FOR
    ## DEF

    def combineOperations(self, cursor, num_to_be_processed, col_names):
        processed_workload = [ ]
        processed_workload_ids = set()
        counter = 0
        error_sess = 0

        while True:
            try:
                next_sess = cursor.next()
                if next_sess['_id'] in self.processed_session_ids:
                    continue
                ## IF

                processed_workload.append(next_sess)
                processed_workload_ids.add(next_sess['_id'])
                counter += 1

                if counter >= num_to_be_processed:
                    break
                ## IF
            except StopIteration:
                break
            except:
                counter += 1
                error_sess += 1
                continue
            ## TRY
        ## WHILE

        combiner = WorkloadCombiner(col_names, processed_workload)
        new_workload = combiner.process(self.design)
        assert new_workload

        return new_workload, error_sess, processed_workload_ids 
    ## FOR

    def migrateDocuments(self):
        if len(self.src2des) == 0:
            return None
        ## IF
        
        start_time = time.time()
        LOG.info("Starting dataset denormalization")
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
        
        LOG.info("Dataset denormalization finished in %s seconds", time.time() - start_time)
    ## DEF
    
    def loadDesign(self): 
        for col_name in self.design.getCollections():
            if self.design.isDenormalized(col_name):
                self.src2des[col_name] = self.design.getDenormalizationParent(col_name)
            ## IF
        ## FOR
    ## DEF
## CLASS