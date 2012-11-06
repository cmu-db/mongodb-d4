#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import logging
import time
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

from search import bbsearch
from search import designcandidates
from search import design

LOG = logging.getLogger(__name__)

'''
BBSearch Unit Tests
Args of BBSearch:
* instance of DesignCandidate: basically dictionary mapping collection names to
  possible shard keys, index keys and collection to denormalize to
* instance of CostModel
* initialDesign (instance of Design)
* upperBound (float; cost of initialDesign)
* timeout (in sec)
'''

class DummyCostModel:
    
    def overallCost(self, design):
        return self.function(design)
    
    def __init__(self, function):
        self.function = function


def checkShardKeyExist(nodelist, shardkey):
    for node in nodelist:
        for col_name in node.design.getCollections():
            key = node.design.getShardKeys(col_name)
            if key == shardkey:
                return True
    return False

def checkIndexKeyExist(nodelist, indexkey):
    for node in nodelist:
        for col_name in node.design.getCollections():
            key = node.design.getIndexes(col_name)
            if key == indexkey:
                return True
    return False


# simple test enumerating all nodes of the search space
class TestSearchSpace (unittest.TestCase) :

    def setUp(self):
        self.initialDesign = design.Design()
        self.initialDesign.addCollection("col1")
        self.initialDesign.addCollection("col2")
        self.initialDesign.reset("col1")
        self.initialDesign.reset("col2")

        self.upper_bound = 1
        self.timeout = 1000000000

        def dummy_bounding_f(design):
            return (0)
        self.costmodel = DummyCostModel(dummy_bounding_f)
    ## DEF

    def outtestSimpleSearch(self):
        '''
        dummy example: since the bounding function returns always float('inf'),
        this should basically traverse the entire tree
        --> this test verifies that bbsearch does not omit any nodes
        '''
        LOG.info("\n\n === BBSearch Simple Test - empty === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], [], [])
        LOG.info("Design Candidates\n%s", dc)
        LOG.info("Initial Design\n%s", self.initialDesign)
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()

        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 3)
        self.assertEqual(bb.leafNodes, 1)

    def outtestShardingKeys(self):
        '''
        now the some with shard keys...
        this should contain 4 leaf nodes, 9 nodes in total
        shard key ([]), ("key1"), ("key2"), ("key1", "key2")
        '''
        LOG.info("\n\n === BBSearch Simple Test - shard keys === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        #for n in nodeList:
        #    print n
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 9)
        self.assertEqual(bb.leafNodes, 4)
        self.assertTrue(checkShardKeyExist(nodeList, ([])))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key2",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1", "key2")))

    def outtestMoreShardingKeys1(self):
        '''
        this should contain
        (3 choose 0) + (3 choose 1) + (3 choose 2) + (3 choose 3) = 8leaf nodes,
        and 8*2 + 1 nodes in total
        '''
        LOG.info("\n\n === BBSearch Simple Test -  more shard keys === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2", "key3"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        #for n in nodeList:
        #    print n
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 17)
        self.assertEqual(bb.leafNodes, 8)
        self.assertTrue(checkShardKeyExist(nodeList, ([])))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key2",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key3",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1", "key2")))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1", "key3")))
        self.assertTrue(checkShardKeyExist(nodeList, ("key2", "key3")))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1", "key2", "key3")))

    def outtestMoreShardingKeys2(self):
        '''
        this should contain
        (5 choose 0) + (5 choose 1) + (5 choose 2) + (5 choose 3) = 26 leaf nodes,
        and 26*2 + 1 = 53 nodes in total
        '''
        LOG.info("\n\n === BBSearch Simple Test -  even more shard keys === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2", "key3", "key4", "key5"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()

        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 53)
        self.assertEqual(bb.leafNodes, 26)


    def outtestIndexes(self):
        '''
        now with some indexes...
        this should contain
        (3 choose 0) + (3 choose 1) + (3 choose 2) + (3 choose 3) = 8leaf nodes,
        and 17 nodes in total
        '''
        LOG.info("\n\n === BBSearch Simple Test - 3 indexes === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [("key1",), ("key1", "key2"), ("key1", "key3")], [], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()

        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 17)
        self.assertEqual(bb.leafNodes, 8)
        self.assertTrue(checkIndexKeyExist(nodeList, ([])))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1",)] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1", "key2")] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1", "key3")] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1",), ("key1", "key2")] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1",), ("key1", "key3")] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1", "key2"), ("key1", "key3")] ))
        self.assertTrue(checkIndexKeyExist(nodeList, [("key1",), ("key1", "key2"), ("key1", "key3")] ))

    def outtestDenormalization(self):
        '''
        now with some denorm...
        3 leaf nodes: no denorm, col1->col2, col2->col1
        '''
        LOG.info("\n\n === BBSearch Simple Test - denorm === \n")

        dc = designcandidates.DesignCandidates()
        dc.addCollection("col1", [], [], ["col2"])
        dc.addCollection("col2", [], [], ["col1"])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, self.costmodel, self.initialDesign, self.upper_bound, self.timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        for n in nodeList:
            print n
            print "*"*50
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(6, bb.totalNodes)
        self.assertEqual(3, bb.leafNodes)
### END Test 1

if __name__ == '__main__':
    unittest.main()