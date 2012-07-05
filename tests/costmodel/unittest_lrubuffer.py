#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import time
import unittest
from pprint import pprint, pformat

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../"))

# mongodb-d4
try:
    from mongodbtestcase import MongoDBTestCase
except ImportError:
    from tests import MongoDBTestCase

import catalog
from costmodel import LRUBuffer
from search import Design
from workload import Session
from util import constants
from util.histogram import Histogram
from inputs.mongodb import MongoSniffConverter

COLLECTION_NAME = "squirrels"
NUM_DOCUMENTS = 1000000
NUM_FIELDS = 6
BUFFER_SIZE = 1048576 # 1 MB

class TestNodeEstimator(unittest.TestCase):

    def setUp(self):
        # Create a fake Collection catalog entry
        # WORKLOAD
        self.col_info = catalog.Collection()
        self.col_info['name'] = COLLECTION_NAME
        self.col_info['doc_count'] = NUM_DOCUMENTS
        self.col_info['workload_queries'] = 1000
        self.col_info['workload_percent'] = 1.0

        for f in xrange(NUM_FIELDS+1):
            # We always need the _id field
            if not f:
                f_name = "_id"
                f_type = catalog.fieldTypeToString(int)
                f_size = catalog.getEstimatedSize(f_type, 10000)
            else:
                f_name = "field%02d" % f
                if f % 2 == 0:
                    f_type = catalog.fieldTypeToString(long)
                    f_size = catalog.getEstimatedSize(f_type, 10000000l)
                else:
                    f_type = catalog.fieldTypeToString(str)
                    f_size = 128

            f = catalog.Collection.fieldFactory(f_name, f_type)
            f['avg_size'] = f_size
            f['query_use_count'] = self.col_info['workload_queries']
            self.col_info['fields'][f_name] = f
            self.col_info['interesting'].append(f_name)
            self.col_info['avg_doc_size'] += f_size
        ## FOR (field)

        self.design = Design()
        self.design.addCollection(self.col_info['name'])
        self.design.addIndex(self.col_info['name'], ["_id"])
        self.design.addIndex(self.col_info['name'], self.col_info['interesting'][1:3])

        self.buffer = LRUBuffer({self.col_info['name']: self.col_info}, BUFFER_SIZE)
        self.buffer.initialize(self.design)
    ## DEF

    def testInitialize(self):
        """Check whether we can initialize the buffer properly for a design"""
        col_name = self.col_info['name']
        self.assertIsNotNone(self.buffer.collection_sizes[col_name])
        self.assertEqual(len(self.design.getIndexes(col_name)), len(self.buffer.index_sizes[col_name]))
        for indexKeys in self.design.getIndexes(col_name):
            self.assertIsNotNone(self.buffer.index_sizes[col_name][indexKeys])
        self.buffer.validate()
    ## DEF

    def testInitializePreloading(self):
        """Check whether preloading the buffer works properly"""

        num_collections = 5
        collections = dict()
        self.design = Design()
        for i in xrange(num_collections):
            col_name = "col%02d" % i
            col_info = catalog.Collection()
            col_info['name'] = col_name
            col_info['doc_count'] = NUM_DOCUMENTS
            col_info['workload_percent'] = 1 / float(num_collections)
            col_info['avg_doc_size'] = 1024
            collections[col_name] = col_info
            self.design.addCollection(col_name)
        ## FOR

        self.buffer = LRUBuffer(collections, BUFFER_SIZE, preload=True)

        try:
            self.buffer.initialize(self.design)
            self.buffer.validate()
        except:
            print self.buffer
            raise
    ## DEF

    def testReset(self):
        """Check whether the LRUBuffer will reset its internal state properly"""
        self.buffer.reset()
        self.assertEqual(BUFFER_SIZE, self.buffer.remaining)
    ## DEF

    def testComputeTupleHash(self):
        num_entries = 10000
        rng = random.Random()
        rng.seed(self.__init__.im_class)
        for i in xrange(num_entries):
            # Construct a tuple and make sure that the size that we get out
            # of it is the size that we put into it
            typeId = rng.choice([LRUBuffer.DOC_TYPE_COLLECTION, LRUBuffer.DOC_TYPE_INDEX])
            key = rng.random()
            size = rng.randint(1, 8) * 1024
            documentId = rng.random()
            buffer_tuple = self.buffer.__computeTupleHash__(typeId, key, size, documentId)
            self.assertIsNotNone(buffer_tuple)

            extracted = self.buffer.__getTupleSize__(buffer_tuple)
            self.assertEqual(size, extracted, pformat(locals())) # "BufferTuple: %d / ExpectedSize: %d" % (buffer_tuple, size))
        ## FOR

    ## DEF

    def testGetDocumentFromCollection(self):
        """Check whether the LRUBuffer updates internal buffer for new collection documents"""

        documentId = 0
        pageHits = 0
        while self.buffer.remaining > self.col_info['avg_doc_size']:
            pageHits += self.buffer.getDocumentFromCollection(self.col_info['name'], documentId)
            before = self.buffer.remaining

            # If we insert the same document, we should not get any pageHits and our
            # remaining memory should be the same
            _pageHits = self.buffer.getDocumentFromCollection(self.col_info['name'], documentId)
            self.assertEqual(0, _pageHits)
            self.assertEqual(before, self.buffer.remaining)

            documentId += 1
            self.buffer.validate()
        ## WHILE

        # We should only have one pageHit per document
        self.assertEqual(documentId, pageHits)

        # Make sure that the buffer is in the right order as we evict records
        lastDocId = None
        while len(self.buffer.buffer) > 0:
            evicted = self.buffer.evictNext(self.col_info['name'])
            self.assertIsNotNone(evicted)
            self.buffer.validate()

            # We can't check this anymore because it's faster for us
            # if we just store the hash of the tuple instead of the
            # actualy tuple values
            # if lastDocId: self.assertLess(lastDocId, docId)
            # lastDocId = docId
        ## WHILE
        self.assertEqual(BUFFER_SIZE, self.buffer.remaining)
    ## DEF

    def testGetDocumentFromIndex(self):
        """Check whether the LRUBuffer updates internal buffer for new index documents"""

        # Roll through each index and add a bunch of documents. Note that the documents
        # will have the same documentId, but they should be represented as separated objects
        # in the internal buffer (because they are for different indexes)
        documentId = 0
        pageHits = 0
        while not self.buffer.evicted:
            for indexKeys in self.design.getIndexes(COLLECTION_NAME):
                pageHits += self.buffer.getDocumentFromIndex(self.col_info['name'], indexKeys, documentId)
                before = self.buffer.remaining

                # If we insert the same document, we should not get any pageHits
                _pageHits = self.buffer.getDocumentFromIndex(self.col_info['name'], indexKeys, documentId)
                self.assertEqual(0, _pageHits)
                self.assertEqual(before, self.buffer.remaining)

                if self.buffer.evicted: break
            documentId += 1
            self.buffer.validate()
        ## WHILE

        # Make sure that we get back two entries for each documentId (except for one)
        lastDocId = None
#        docIds_h = Histogram()
        while len(self.buffer.buffer) > 0:
#            typeId, key, docId = self.buffer.evictNext(COLLECTION_NAME)
            evicted = self.buffer.evictNext(COLLECTION_NAME)
            self.assertIsNotNone(evicted)
            self.buffer.validate()
#            self.assertIsNotNone(typeId)
#            self.assertIsNotNone(key)
#            self.assertIsNotNone(docId)
#            docIds_h.put(docId)
        ## WHILE

#        foundSingleDocId = False
#        for documentId,cnt in docIds_h.iteritems():
#            if cnt == 1:
#                self.assertFalse(foundSingleDocId)
#                foundSingleDocId = True
#            else:
#                self.assertEqual(2, cnt)
#        ## FOR

        self.assertEqual(BUFFER_SIZE, self.buffer.remaining)
    ## DEF


## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN