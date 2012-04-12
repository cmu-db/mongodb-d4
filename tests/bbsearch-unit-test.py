#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import bbsearch
from search import designcandidate
from search import design
import time

'''
BBSearch Unit Tests
Args of BBSearch:
* instance of DesignCandidate: basically dictionary mapping collection names to possible shard keys, index keys and collection to denormalize to
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
        for col in node.design.collections:
            key = node.design.shardKeys[col]
            if key == shardkey:
                return True
    return False

def checkIndexKeyExist(nodelist, indexkey):
    for node in nodelist:
        for col in node.design.collections:
            key = node.design.indexes[col]
            if key == indexkey:
                return True
    return False


# simple test enumerating all nodes of the search space
class TestSearchSpace (unittest.TestCase) :
    def testBBSearch1(self):
        def dummy_bounding_f(design):
            return (0)
        initialDesign = design.Design()
        upper_bound = 1
        timeout = 1000000000    
        costmodel = DummyCostModel(dummy_bounding_f)
        
        '''
        dummy example: since the bounding function returns always float('inf'),
        this should basically traverse the entire tree
        --> this test verifies that bbsearch does not omit any nodes
        '''
        print("\n\n === BBSearch Simple Test - empty === \n")
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], [], [])
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 3)
        self.assertEqual(bb.leafNodes, 1)
        
        '''
        now the some with shard keys...
        this should contain 4 leaf nodes, 9 nodes in total
        shard key ([]), ("key1"), ("key2"), ("key1", "key2")
        '''
        print("\n\n === BBSearch Simple Test - shard keys === \n")
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        for n in nodeList:
            print n
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 9)
        self.assertEqual(bb.leafNodes, 4)
        self.assertTrue(checkShardKeyExist(nodeList, ([])))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key2",)))
        self.assertTrue(checkShardKeyExist(nodeList, ("key1", "key2")))
        
        
        print("\n\n === BBSearch Simple Test -  more shard keys === \n")
        '''
        this should contain
        (3 choose 0) + (3 choose 1) + (3 choose 2) + (3 choose 3) = 8leaf nodes,
        and 8*2 + 1 nodes in total
        '''
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2", "key3"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        for n in nodeList:
            print n
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
   
        print("\n\n === BBSearch Simple Test -  even more shard keys === \n")
        '''
        this should contain
        (5 choose 0) + (5 choose 1) + (5 choose 2) + (5 choose 3) = 26 leaf nodes,
        and 26*2 + 1 = 53 nodes in total
        '''
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [], ["key1", "key2", "key3", "key4", "key5"], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 53)
        self.assertEqual(bb.leafNodes, 26)
        
   
   
        '''
        now with some indexes...
        this should contain 
        (3 choose 0) + (3 choose 1) + (3 choose 2) + (3 choose 3) = 8leaf nodes,
        and 17 nodes in total
        '''
        print("\n\n === BBSearch Simple Test - 3 indexes === \n")
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], [])
        dc.addCollection("col2", [("key1"), ("key1", "key2"), ("key1", "key3")], [], [])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 17)
        self.assertEqual(bb.leafNodes, 8)
        self.assertTrue(checkIndexKeyExist(nodeList, ([])))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1",)) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1", "key2"),) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1", "key3"),) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1"), ("key1", "key2")) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1"), ("key1", "key3")) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1", "key2"), ("key1", "key3")) ))
        self.assertTrue(checkIndexKeyExist(nodeList, (("key1"), ("key1", "key2"), ("key1", "key3")) ))
        
        
        '''
        now with some denorm...
        3 leaf nodes: no denorm, col1->col2, col2->col1
        '''
        print("\n\n === BBSearch Simple Test - denorm === \n")
        dc = designcandidate.DesignCandidate()
        dc.addCollection("col1", [], [], ["col2"])
        dc.addCollection("col2", [], [], ["col1"])
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
        bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        for n in nodeList:
            print n
        self.assertEqual(bb.totalNodes, len(nodeList))
        self.assertEqual(bb.totalNodes, 6)
        self.assertEqual(bb.leafNodes, 3)

### END Test 1
        


if __name__ == '__main__':
    unittest.main()