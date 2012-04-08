# -*- coding: utf-8 -*-

import sys
import json
import logging

## ==============================================
## CostModel
## ==============================================
class CostModel(object):
    
    def __init__(self, constants) :
        self.alpha = constants['alpha']
        self.beta = constants['beta']
        self.gamma = constants['gamma']
        
    def overallCost(self, design, workload, config) :
        cost = 0
        cost += self.alpha * self.networkCost(design, workload, config['nodes'])
        cost += self.beta * self.diskCost(design, workload)
        cost += self.gamma * self.skewCost(design, workload)
        return cost
        
    def networkCost(self, design, workload, nodes) :
        result = 0
        for s in workload.sessions :
            for q in s.queries :
                # Determine nodes touched by query
        return 1.0
        
    def diskCost(self, design, workload):
        return 1.0
        
    def skewCost(self, design, workload):
        '''
        Temporal skew costs
        Break workload up into segments.  Measure distribution of queries across nodes
        during each segment to determine overall temporal skew
        '''
        return 1.0
## CLASS
    