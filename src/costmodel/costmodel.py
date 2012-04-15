# -*- coding: utf-8 -*-

import sys
import json
import logging
import math 

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
            
        # Divide the workload up into segments for skew analysis
        offset = (end - start) / self.skew_segments
        timer = start + offset
        i = 0
        wl_seg = self.workload.factory()
        for s in self.workload.sessions :
            if s.endTime > timer :
                i += 1
                timer += offset
                segments.append(wl_seg)
                wl_seg = self.workload.factory()
            wl_seg.addSession(s)
        segments.append(wl_seg)
        
        # Calculate the network cost for each segment for skew analysis
        for i in range(0, len(segments)) :
            segment_costs.append(self.partialNetworkCost(design, segments[i]))
        
        # Determine overall skew cost as a function of the distribution of the
        # segment network costs
        return CostModel.skewVariance(segment_costs)
        
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
        
    @staticmethod
    def skewVariance(list) :
        norm = max(list)
        n, mean, std = len(list), 0, 0
        if n <= 1 or norm == 0 :
            return 0
        else :
            for a in list:
                mean = mean + a
            mean = mean / float(n)
            for a in list:
                std = std + (a - mean)**2
            std = math.sqrt(std / float(n-1))
        return abs(1 - (std / norm))
## CLASS
    