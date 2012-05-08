#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
import workload
import datetime
import costmodel
import search

## ==============================================
## main
## ==============================================

if __name__ == '__main__':

    config = {
        'alpha' : 1.0,
        'beta' : 1.0,
        'gamma' : 1.0,
        'nodes' : 4,
        'max_memory' : 1024,
        'address_size' : 64,
        'skew_intervals' : 10,
    }
    
    statistics = {
        'A' : {
            'fields' : {
                'col1' : {
                    'query_use_count' : 1000,
                    'cardinality' : 500,
                    'selectivity' : 0.5,
                },
                'col2' : {
                    'query_use_count' : 1000,
                    'cardinality' : 100,
                    'selectivity' : 1.0,
                },
                'col3' : {
                    'query_use_count' : 1000,
                    'cardinality' : 250,
                    'selectivity' : 0.25,
                }
            },
            'tuple_count' : 100000,
            'workload_queries' : 1000,
            'workload_percent' : 100,
            'kb_per_doc' : 10,
            'max_pages' : 50,
        },
        'total_queries' : 1000,
    }

    ## ----------------------------------------------
    ## STEP 1
    ## Network Cost Evaluation
    ## ----------------------------------------------
    print 'Evaluating Network Cost (1000 operations, 4 shards)'
    
    wk = workload.Workload()
    sess = workload.SyntheticSession()
    sess.startTime = 0
    sess.endTime = 1000
    ts = 0
    for i in range(0, 1000) :
        q = workload.Query()
        q.collection = 'A'
        q.type = 'select'
        if i % 2 == 1 :
            q.predicates = {'col1' : 'equality', 'col2' : 'equality'}
        else :
            q.predicates = {'col1' : 'equality', 'col2' : 'range'}
        q.projection = {}
        q.timestamp = ts + i
        sess.queries.append(q)
    wk.addSession(sess)
    
    cm = costmodel.CostModel(wk, config, statistics)
    
    print '** TEST 1: All ops executed at 1 shard '
    d1 = search.Design()
    d1.addCollection('A')
    d1.addShardKey('A', ['col1'])
    print cm.networkCost(d1)
    
    print '** TEST 2: All ops executed at 2 shards '
    d2 = search.Design()
    d2.addCollection('A')
    d2.addShardKey('A', ['col2'])
    print cm.networkCost(d2)
    
    print '** TEST 3: All ops executed at 4 shards '
    d3 = search.Design()
    d3.addCollection('A')
    d3.addShardKey('A', ['col3'])
    print cm.networkCost(d3)
    
    ## ----------------------------------------------
    ## STEP 2
    ## Skew Cost Evaluation
    ## ----------------------------------------------
    
    ## -------------------------------------------------
    ## STEP 3
    ## Disk Cost evaluation
    ## -------------------------------------------------
    
    pass
## MAIN