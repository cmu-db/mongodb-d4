#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import random
import time
import unittest
from pprint import pprint

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

    def testGetDocumentsFromCollection(self):
        """Check whether the LRUBuffer updates internal buffer for new collection documents"""

        documentId = 0
        pageHits = 0
        while self.buffer.remaining > self.col_info['avg_doc_size']:
            pageHits += self.buffer.getDocumentsFromCollection(self.col_info['name'], [documentId])
            documentId += 1
        ## WHILE

        # We should only have one pageHit per document
        self.assertEqual(documentId, pageHits)

        # Make sure that the buffer is in the right order as we evict records
        lastDocId = None
        while len(self.buffer.buffer) > 0:
            typeId, key, docId = self.buffer.evictNext()
            self.assertIsNotNone(typeId)
            self.assertIsNotNone(key)
            self.assertIsNotNone(docId)
            if lastDocId: self.assertLess(lastDocId, docId)
            lastDocId = docId
        ## WHILE
        self.assertEqual(BUFFER_SIZE, self.buffer.remaining)
    ## DEF

    def testEvictNext(self):
        """Check whether the LRUBuffer can evict documents and free space properly"""
        pass
    ## DEF

    def testReset(self):
        """Check whether the LRUBuffer will reset its internal state properly"""
        self.buffer.reset()
        self.assertEqual(BUFFER_SIZE, self.buffer.remaining)
    ## DEF

    def testInitialize(self):
        """Check whether we can initialize the buffer properly for a design"""
        self.assertIsNotNone(self.buffer.collection_sizes[self.col_info['name']])
        self.assertEqual(len(self.design.getIndexes(self.col_info['name'])), len(self.buffer.index_sizes))
        for indexKeys in self.design.getIndexes(self.col_info['name']):
            self.assertIsNotNone(self.buffer.index_sizes[indexKeys])
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN