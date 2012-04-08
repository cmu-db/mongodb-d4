# -*- coding: utf-8 -*-

import sys
import json
import logging

## ==============================================
## CostModel
## ==============================================
class CostModel(object):
    
    alpha = None
    beta = None
    gamma = None
    
    def __init__(self, constants) :
        self.alpha = constants['alpha']
        self.beta = constants['beta']
        self.gamma = constants['gamma']
        
    def overallCost(self, design, workload, config):
        assert self != None
        # TODO
    
    def networkCost(self, design, workload):
        assert self != None
        
    def diskCost(self, design, workload):
        assert self != None
        
    def skewCost(self, design, workload):
        assert self != None
        # TODO
        
## CLASS
    