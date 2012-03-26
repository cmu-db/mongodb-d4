#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from search import bbsearch

def testBBSearch1():
    '''
    dummy example: since the bounding function returns always float('inf'),
    this should basically traverse the entire tree
    --> this test verifies that bbsearch does not omit any nodes
    '''
    def dummy_bounding_f(design):
        return float('inf')
        
    collections = {"col1": ["shardkey1", "shardkey2"], "col2": ["id"], "col3": ["id"]}
    init_design = bbsearch.BBDesign(collections)
    
    timeout = 1000000000
    
    bb = bbsearch.BBSearch(init_design, dummy_bounding_f, timeout)
    bb.solve()
    nodeList = bb.listAllNodes()
    
    for n in nodeList:
        print n
        

def testBBSearch2():
    
    '''
    more complicated example:
    '''
    
    def cost_function(design):
        
        return 
    
    

if __name__ == '__main__':
    testBBSearch1()
