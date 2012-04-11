# -*- coding: utf-8 -*-

import sys
import json
import logging

## ==============================================
## CostModel
## ==============================================
class CostModel(object):
    
    def __init__(self, constants, statistics = {}) :
        self.alpha = constants['alpha']
        self.beta = constants['beta']
        self.gamma = constants['gamma']
        self.stats = statistics
        
    def overallCost(self, design, workload, config) :
        cost = 0
        cost += self.alpha * self.networkCost(design, workload, config['nodes'])
        cost += self.beta * self.diskCost(design, workload)
        cost += self.gamma * self.skewCost(design, workload)
        return cost
        
    def networkCost(self, design, workload, nodes) :
        result = 0
        stat_collections = list(self.stats)
        for s in workload.sessions :
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
        
    def diskCost(self, design, workload):
        return 1.0
        
    def skewCost(self, design, workload):
        result = 0
        for s in workload.sessions :
            for q in s.queries :
                pass
        return result
## CLASS
    