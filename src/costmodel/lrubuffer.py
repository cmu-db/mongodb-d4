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
from pprint import pformat
from util import constants

LOG = logging.getLogger(__name__)

class LRUBuffer:
    DOC_TYPE_INDEX = 0
    DOC_TYPE_COLLECTION = 1

    def __init__(self, collections, buffer_size):
#        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.collections = collections
        self.collection_sizes = { }
        self.index_sizes = { }
        self.design = None

        # The buffer is just a list of uniquely identifable objects
        # The elements are ordered from the least used to most recent
        self.buffer = [ ]

        # This is the total amount of space available in this buffer (bytes)
        self.buffer_size = buffer_size
        # This is the amount of space that is unallocated in this buffer (bytes)
        self.remaining = buffer_size
        self.evicted = 0

        self.address_size = constants.DEFAULT_ADDRESS_SIZE # FIXME

        pass
    ## DEF

    def reset(self):
        """
            Reset the internal buffer and "free" all of its used memory
        """
        self.remaining = self.buffer_size
        self.buffer = [ ]
        self.evicted = 0
        self.collection_sizes = { }
        self.index_sizes = { }
    ## DEF

    def initialize(self, design):
        """
            Add the given collection to our buffer.
            This will automatically initialize any indexes for the collection as well.
        """
        self.reset()
        for col_name in design.getCollections():
            col_info = self.collections[col_name]
            self.collection_sizes[col_name] = col_info['avg_doc_size']

            # For each collection, get the indexes that are included in the design.
            col_index_sizes = { }
            for index_keys in design.getIndexes(col_name):
                col_index_sizes[index_keys] = self.getIndexSize(col_info, index_keys)
            self.index_sizes[col_name] = col_index_sizes
        self.design = design
    ## DEF

    def getDocumentsFromIndex(self, col_name, indexKeys, documentIds):
        """
            Get the documents from the given index
            Returns the number of page hits incurred to read these documents.
        """
        size = self.index_sizes[col_name].get(indexKeys, 0)
        assert size > 0, \
            "Missing index size for %s -> '%s'\n%s" % (col_name, indexKeys, pformat(self.index_sizes))
        return self.getDocuments(LRUBuffer.DOC_TYPE_INDEX, col_name, indexKeys, size, documentIds)
    ## DEF

    def getDocumentsFromCollection(self, col_name, documentIds):
        """
            Get the documents from the given index
            Returns the number of page hits incurred to read these documents.
        """
        size = self.collection_sizes.get(col_name, 0)
        assert size > 0, \
            "Missing collection size for '%s'" % col_name
        return self.getDocuments(LRUBuffer.DOC_TYPE_COLLECTION, col_name, col_name, size, documentIds)
    ## DEF

    def getDocuments(self, typeId, col_name, key, size, documentIds):
        page_hits = 0
        for documentId in documentIds:
            buffer_tuple = (typeId, key, documentId)

            # The tuple is in our buffer, so we don't need to fetch anything from disk
            # We will need to push the tuple back on to the end of our buffer list
            if buffer_tuple in self.buffer:
                self.buffer.remove(buffer_tuple)
                self.buffer.append(buffer_tuple)

            # It's not in the buffer for this index, so we're going to have
            # go fetch it from disk. Check whether we can just fetch
            # the page in or whether we will need to write out a dirty page right now
            # NOTE: We actually don't know whether the page is dirty. If it wasn't then
            #       OS would just discard it rather than write it back to disk. Figuring
            #       that out would be too expensive for this estimate, since we don't
            #       actually know how many documents are in that page that it needs to write
            else:
                assert size < self.buffer_size
                while (self.remaining - size) < 0:
                    if self.debug:
                        LOG.info("Buffer is full. Evicting documents to gain %d bytes [remaining=%d]", \
                                 size, self.remaining)
                    self.evictNext(col_name)
                    page_hits += 1

                self.buffer.append(buffer_tuple)
                self.remaining -= size
                page_hits += 1
        ## FOR (document)
        return page_hits
    ## DEF

    def evictNext(self, col_name):
        typeId, key, docId = self.buffer.pop(0)
        if typeId == LRUBuffer.DOC_TYPE_INDEX:
            size = self.index_sizes[col_name][key]
        elif typeId == LRUBuffer.DOC_TYPE_COLLECTION:
            size = self.collection_sizes[key]
        else:
            raise Exception("Unexpected LRUBuffer type id '%s'" % typeId)
        self.remaining += size
        if self.debug:
            LOG.debug("Evicted document - Remaining:%d", self.remaining)

        self.evicted += 1
        return typeId, key, docId
    ## DEF

    def getIndexSize(self, col_info, indexKeys):
        """Estimate the amount of memory required by the indexes of a given design"""
        # TODO: This should be precomputed ahead of time. No need to do this
        #       over and over again.
        index_size = 0
        for f_name in indexKeys:
            f = col_info.getField(f_name)
            assert f, "Invalid index key '%s.%s'" % (col_info['name'], f_name)
            index_size += f['avg_size']
        index_size += self.address_size
        if self.debug: LOG.debug("%s Index %s Memory: %d bytes",\
            col_info['name'], repr(indexKeys), index_size)
        return index_size
    ## DEF
## CLASS