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
        
    def overallCost(self, design, workload, config):
        cost = 0
        cost += self.alpha * self.networkCost(design, workload)
        cost += self.beta * self.diskCost(design, workload)
        cost += self.gamma * self.skewCost(design, workload)
        return cost
        
    def networkCost(self, design, workload):
        return 1.0
        
    def diskCost(self, design, workload):
        return 1.0
        
    def skewCost(self, design, workload):
        return 1.0
## CLASS
    