#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import design
from search import bbsearch
from search import designcandidate

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
        
        

# simple test enumerating all nodes of the search space
def testBBSearch1():
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
    
    
    
    '''
    now the some with shard keys...
    '''
    print("\n\n === BBSearch Simple Test - shard keys === \n")
    dc = designcandidate.DesignCandidate()
    dc.addCollection("col1", [], [], [])
    dc.addCollection("col2", [], ["key1", "key2"], [])
    #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
    bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    
    print("\n\n === BBSearch Simple Test -  more shard keys === \n")
    dc = designcandidate.DesignCandidate()
    dc.addCollection("col1", [], ["f1", "f2"], [])
    dc.addCollection("col2", [], ["key2", "key3"], [])
    #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
    bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    
    '''
    now with some indexes...
    '''
    print("\n\n === BBSearch Simple Test - indexes === \n")
    dc = designcandidate.DesignCandidate()
    dc.addCollection("col1", [], [], [])
    dc.addCollection("col2", [("key1"), ("key1", "key2"), ("key1", "key3")], [], [])
    #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
    bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    '''
    now with some denorm...
    '''
    print("\n\n === BBSearch Simple Test - denorm === \n")
    dc = designcandidate.DesignCandidate()
    dc.addCollection("col1", [], [], ["col2"])
    dc.addCollection("col2", [], [], ["col1"])
    #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..3,None
    bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    

### END Test 1
        

# Timout test
def testBBSearch_timeoutTest():
    print("\n\n === BBSerch Timeout Test === \n")
    
    # cost function takes 1 sec... therefore, there should be about 10 nodes after 10 sec
    def slow_bounding_f(design):
        time.sleep(1)
        return 0
        
    initialDesign = design.Design()
    upper_bound = 1
    timeout = 5
    costmodel = DummyCostModel(slow_bounding_f)
    
    dc = designcandidate.DesignCandidate()
    dc.addCollection("col1", [], ["key1", "key2", "key3"], [])
    dc.addCollection("col2", [], ["field1", "field2"], [])
    bb = bbsearch.BBSearch(dc, costmodel, initialDesign, upper_bound, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()


### END Timeout Test   
    
'''
# "real" test - finding a solution using some heuristic function...
def testBBSearch2():
    print("\n\n === BBSerch Test 2 ===\n")
    operations = []
    operations.append(("query", "col1", "shardkey1"));
    operations.append(("query", "col1", "shardkey1"));
    operations.append(("query", "col1", "shardkey1"));
    operations.append(("query", "col1", "shardkey1"));
    operations.append(("query", "col1", "shardkey2"));
    operations.append(("query", "col1", "shardkey2"));
    operations.append(("query", "col2", "id"));
    operations.append(("query", "col2", "id"));
    operations.append(("join", "col2", "col3"));
    operations.append(("join", "col2", "col3"));
    operations.append(("join", "col2", "col3"));
    operations.append(("join", "col2", "col3"));

    
    def bounding_f(design):
        totalq = 0.0
        query_subcost = 0.0
        query_subcost_low = 0.0
        totalj = 0.0
        join_subcost = 0.0
        join_subcost_low = 0.0
        for o in operations:
            if o[0]=="query":
                totalq += 1
                col = o[1]
                key = o[2]
                query_subcost_low += 0.5
                if design.assignment[col] == None:
                    query_subcost += 1.0
                else:
                    if key==design.assignment[col][0]:
                        query_subcost+=0.5
                    else:
                        query_subcost+=1.0
                
            if o[0]=="join":
                totalj += 1
                col1=o[1]
                col2=o[2]
                join_subcost_low += 0.5
                if (design.assignment[col2] == None) and (design.assignment[col1] == None):
                    join_subcost += 1.0
                else:  
                    if (design.assignment[col2] != None) and (col1==design.assignment[col2][1]):
                        join_subcost += 0.5
                    else:
                        join_subcost += 0.8
                    if (design.assignment[col1] != None) and (col2==design.assignment[col1][1]):
                        join_subcost += 0.5
                    else:
                        join_subcost += 0.8
                
                        
        cost = float(join_subcost) / totalj + float(query_subcost) / totalq
        cost_low = float(join_subcost_low) / totalj + float(query_subcost_low) / totalq
        return (cost_low, cost)
        
    collections = {"col1": ["shardkey1", "shardkey2"], "col2": ["id"], "col3": ["id"]}
      
    # TIMEOUT = 10sec
    timeout = 10
    
    bb = bbsearch.BBSearch(collections, bounding_f, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    print "Total nodes: ", len(nodeList)
    
    #for n in nodeList:
    #    print n

### END Test 2



# test to prevent circular embedding
def testBBSearch3():
    print("\n\n === BBSerch Test 3 - circular embedding ===\n")
    
    
    without special checking, the following workload will
    lead to something like col1-->col2-->col3-->col1
    
    
    operations = []
    operations.append(("query", "col1", "id"));
    operations.append(("join", "col2", "col3"));
    operations.append(("join", "col1", "col2"));
    operations.append(("join", "col3", "col1"));
    

    
    def bounding_f(design):
        totalq = 0.0
        query_subcost = 0.0
        query_subcost_low = 0.0
        totalj = 0.0
        join_subcost = 0.0
        join_subcost_low = 0.0
        for o in operations:
            if o[0]=="query":
                totalq += 1
                col = o[1]
                key = o[2]
                query_subcost_low += 0.5
                if design.assignment[col] == None:
                    query_subcost += 1.0
                else:
                    if key==design.assignment[col][0]:
                        query_subcost+=0.5
                    else:
                        query_subcost+=1.0
                        query_subcost_low += 0.5
                
            if o[0]=="join":
                totalj += 1
                col1=o[1]
                col2=o[2]
                join_subcost_low += 0.5
                if ((design.assignment[col2] == None) and (design.assignment[col1] == None)):
                    join_subcost += 1.0
                else:
                    if design.assignment[col2] != None:
                        if col1==design.assignment[col2][1]:
                            join_subcost += 0.5
                        else:
                            join_subcost += 0.8
                            join_subcost_low += 0.3
                    else:
                        if col2==design.assignment[col1][1]:
                            join_subcost += 0.5
                        else:
                            join_subcost += 0.8
                            join_subcost_low += 0.3

                
                        
        cost = float(join_subcost) / totalj + float(query_subcost) / totalq
        cost = cost / 2
        cost_low = float(join_subcost_low) / totalj + float(query_subcost_low) / totalq
        cost_low = cost_low / 2
        return (cost_low, cost)
        
    collections = {"col1": ["id"], "col2": ["id"], "col3": ["id"]}
      
    # TIMEOUT = 10sec
    timeout = 10
    
    bb = bbsearch.BBSearch(collections, bounding_f, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    print "Total nodes: ", len(nodeList)
    
    #for n in nodeList:
    #    print n

### END Test 3
'''

if __name__ == '__main__':
    testBBSearch1()
    testBBSearch_timeoutTest()
    #testBBSearch2()
    #testBBSearch3()