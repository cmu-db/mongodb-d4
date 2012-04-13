# -*- coding: utf-8 -*-

import sys
import json
import logging

## ==============================================
## CostModel
## ==============================================
class CostModel(object):
    
    def __init__(self, workload, config, statistics = {}) :
        self.workload = workload
        self.alpha = config['alpha']
        self.beta = config['beta']
        self.gamma = config['gamma']
        self.nodes = config['nodes']
        self.stats = statistics
        
    def overallCost(self, design) :
        cost = 0
        cost += self.alpha * self.networkCost(design)
        cost += self.beta * self.diskCost(design)
        cost += self.gamma * self.skewCost(design)
        return cost
        
    def networkCost(self, design) :
        result = 0
        stat_collections = list(self.stats)
        for s in self.workload.sessions :
            for q in s.queries :
                if q.type == 'insert' :
                    result += 1
                elif q.type == 'select' :
                    if len(q.predicates) > 0 :
                        scan = True
                        for k,v in q.predicates.iteritems() :
                            if design.shardKeys[q.collection] == k :
                                scan = False
                        if scan == False : 
                            result += 1
                        else :
                            reslut += 10
                elif q.type == 'update' :
                    pass
                elif q.type == 'delete' :
                    pass
                
        return result
        
    def diskCost(self, design):
        return 1.0
        
    def skewCost(self, design):
        result = 0
        for s in self.workload.sessions :
            for q in s.queries :
                pass
        return result
## CLASS
    