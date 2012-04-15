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
        self.skew_segments = 10
        
    def overallCost(self, design) :
        cost = 0
        cost += self.alpha * self.networkCost(design)
        cost += self.beta * self.diskCost(design)
        cost += self.gamma * self.skewCost(design)
        return cost
        
    def networkCost(self, design) :
        return self.partialNetworkCost(design, self.workload)
        
    def diskCost(self, design):
        return 1.0
        
    def skewCost(self, design):
        segment_costs = []
        segments = []
        if self.workload.length > 0 :
            start = self.workload.sessions[0].startTime
            end = self.workload.sessions[self.workload.length - 1].endTime
        else :
            return 0
        offset = (end - start) / self.skew_segments
        for s in self.workload.sessions :
            pass
        return -1
        
    def partialNetworkCost(self, design, wrkld_sgmnt) :
        result = 0
        stat_collections = list(self.stats)
        for s in wrkld_sgmnt.sessions :
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
                            result += 10
                elif q.type == 'update' :
                    pass
                elif q.type == 'delete' :
                    pass
        return result
## CLASS
    