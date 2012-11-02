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

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))

# mongodb-d4
sys.path.append(os.path.join(basedir, ".."))
import catalog
import workload
from catalog import Collection
from workload import Session
from util import Histogram
from util import constants

LOG = logging.getLogger(__name__)

class Normalizer:
    """MongoDB Database Normalizer"""

    def __init__(self, metadata_db, dataset_db):
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.metadata_db = metadata_db
        self.dataset_db = dataset_db

        ## DEF

    def process(self):
        """Iterates through all operations of all sessions and normalizs the dataset..."""
        # In this method, we need to go through every document in every collection to see if it is possible to extract
        # collections

        # DON'T FORGET TO UPDATE THE OPERATIONS AS WELL
        LOG.info("Normalizing Database")
        # In order not to affect the traversing process, we create a two batches, one is for deleted fields
        # the other is for new collections
        changed_fields = [ ]
        for col_name in self.dataset_db.collection_names():
            # Skip ignored collections
            if col_name.split(".")[0] in constants.IGNORED_COLLECTIONS:
                continue
            LOG.info("Trying to extract collections from collection '%s'", col_name)

            # Examine each document in the dataset for this collection
            for doc in self.dataset_db[col_name].find():
                try:
                    self.processDocData(col_name, doc, changed_fields)
                except:
                    msg = "Unexpected error when processing '%s' data fields" % col_name
                    msg += "\n" + pformat(doc)
                    LOG.error(msg)
                    raise
            ## FOR

        LOG.info("Reconstructing dataset...")
        fieldscol2col = self.reconstructDataset(changed_fields)
        self.reconstructMetaData(changed_fields, fieldscol2col)
    ## DEF

    def processDocData(self, parent_col, doc, changed_fields):
        """
        Recursively traverse a single document and extract out the field information
        """
        if self.debug: LOG.debug("Examing fields for document:\n%s" % pformat(doc))
        for k,v in doc.iteritems():
            # Skip if this is the _id field
            if constants.SKIP_MONGODB_ID_FIELD and k == '_id': continue

            f_type = type(v)
            if f_type == list and len(v) > constants.MIN_SIZE_OF_NESTED_FIELDS:
                # If we find a qualified sub-collection
                # 1. Delete it from the old collection
                # 2. Add it to the dataset as a new collection
                changed_fields.append((parent_col, k, v))
            ## IF
        ## FOR
    ## DEF

    def reconstructDataset(self, changed_fields):
        """
        We have got the fields should be deleted, now it is time to reconstruct the dataset
        """
        if self.debug: LOG.info("Reconstructing dataset!")
        fieldscol2col = { }
        # First, remove the fields in changed_fields
        for field in changed_fields:
            col_name = field[0]
            payload = {field[1] : field[2]}
            if self.debug: LOG.debug("Deleting documents from collection %s..", col_name)
            self.dataset_db[col_name].remove(payload)
        # Then we add the fields to dataset as new collections
        for field in changed_fields:
            col_name = field[0] + "__" + field[1]
            if not field[1] in fieldscol2col:
                fieldscol2col[(field[0], field[1])] = col_name

            payload = {'parent_col' : field[0], field[1] : field[2]}
            # we should refer this collection back to its parent collection
            if self.debug: LOG.debug("Adding collection %s to dataset..", col_name)
            self.dataset_db[col_name].save(payload)
        ## FOR

        return fieldscol2col
    ## DEF

    def reconstructMetaData(self, changed_fields, fieldscol2col):
        """
        Since we have re-constructed the database
        """
        col2fields = self.generateDict(changed_fields)
        for sess in self.metadata_db.Session.fetch():
            for i in xrange(len(sess['operations']) - 1, - 1, -1):
                offset = 1 # indicate where we should insert the splitted operation. It depends on if we remove the current operation
                op = sess['operations'][i]
                col_name = op['collection']
                fields = col2fields.get(col_name, None)
                # If this op's collection has no fields in moved_fields, skip it
                if fields:
                    payload = op["query_content"] # payload is a list type
                    changed_query = [ ]
                    counter = 0
                    while counter < len(payload):
                        doc = payload[counter] # doc is a dict type
                        for key in doc.iterkeys():
                            if key in fields:
                                changed_query.append((counter, key))
                            ## IF
                        ## FOR
                    ## WHILE
                    # If we have queries to split
                    if len(changed_query) > 0:
                        # construct new queries
                        for tup in changed_query:
                            old_query_content = payload[tup[0]].pop(tup[1])
                            # If the doc is empty after the pop, remove it from the payload
                            if len(payload[tup[0]]) == 0:
                                payload.remove(payload[tup[0]])
                                # If the payload is empty, we remove the op from the session queue
                                if len(payload) == 0:
                                    sess.remove(op)
                                    offset -= 1
                                ## IF
                            ## IF
                            new_op = Session.operationFactory()

                            new_op['collection'] = fieldscol2col[(col_name, tup[1])]
                            new_op['type']  = op['type']
                            ## new_op['query_id']      = queryId
                            new_op['query_content'] = [ old_query_content ]
                            #new_op['resp_content']  = [ responseContent ]
                            #new_op['resp_id']       = responseId
                            #new_op['predicates']    = queryPredicates
                            new_op['query_time']    = op['query_time']
                            new_op['resp_time']    = op['resp_time']

                            # add the new query after the current one of the session queue
                            sess.insert(i + offset, new_op)
                        ## FOR
                    ## IF
                ## IF
            ## FOR
        ## FOR
    ## DEF

    def generateDict(self, changed_fields):
        """
        Generate a map from collection -> fields, which simplifies the metadata reconstruction process
        """
        col2fields = { }
        for field in changed_fields:
            col_name = field[0]
            field_name = field[1]
            if not col_name in col2fields:
                col2fields[col_name] = set()
            ## IF
            col2fields[col_name].add(field_name)
        ## FOR
        return col2fields
    ## DEF