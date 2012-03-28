#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import bbsearch
import time

'''
BBSearch Unit Tests
'''


# simple test enumerating all nodes of the search space
class TestSearchSpace (unittest.TestCase) :
    def testBBSearch1(self):
        print("\n\n === BBSearch Simple Test === \n")
        '''
        dummy example: since the bounding function returns always float('inf'),
        this should basically traverse the entire tree
        --> this test verifies that bbsearch does not omit any nodes
        '''
        def dummy_bounding_f(design):
            return (0, float('inf'))
            
        collections = {"col1": [], "col2": []}
        
        timeout = 1000000000
        
        bb = bbsearch.BBSearch(collections, dummy_bounding_f, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        
        # 3 leaf nodes
        self.assertEqual(bb.leafNodes, 3)
        
        # 6 nodes
        self.assertEqual(len(nodeList), 6)
        
        '''
        now the same with shard keys...
        '''
        collections = {"col1": ["key1", "key2", "key3"], "col2": []}
        #same as above, just 4 time more leaf nodes, since c1 can be sharded on k1..4,None
        bb = bbsearch.BBSearch(collections, dummy_bounding_f, timeout)
        bb.solve()
        nodeList = bb.listAllNodes()
        
        # 12 leaf nodes
        self.assertEqual(bb.leafNodes, 12)
    
    ### END Test 1
        

    
    
    # "real" test - finding a solution using some heuristic function...
    def testBBSearch2(self):
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
          
        timeout = 10
        
        bb = bbsearch.BBSearch(collections, bounding_f, timeout)
        result = bb.solve()
        nodeList = bb.listAllNodes()
        
        solution = {'col2': ('id', None), 'col3': (None, 'col2'), 'col1': ('shardkey1', None)}
        self.assertEqual(solution, result)
    
    ### END Test 2


    # test to prevent circular embedding
    def testBBSearch3(self):
        print("\n\n === BBSerch Test 3 - circular embedding ===\n")
        
        '''
        without special checking, the following workload will
        lead to something like col1-->col2-->col3-->col1
        '''
        
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
        solution = bb.solve()
        nodeList = bb.listAllNodes()
        
        print "Total nodes: ", len(nodeList)
        
        wrong_solution1 = {'col2': (None, 'col1'), 'col3': (None, 'col2'), 'col1': ('id', 'col3')}
        wrong_solution2 = {'col2': (None, 'col3'), 'col3': (None, 'col1'), 'col1': ('id', 'col2')}
    
        self.assertNotEqual(solution, wrong_solution1)
        self.assertNotEqual(solution, wrong_solution2)
        
    
    ### END Test 3


if __name__ == '__main__':
    unittest.main()