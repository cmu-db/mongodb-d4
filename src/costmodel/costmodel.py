# -*- coding: utf-8 -*-
from __future__ import division
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
        self.skew_segments = 9
        
    def overallCost(self, design) :
        cost = 0
        cost += self.alpha * self.networkCost(design)
        cost += self.beta * self.diskCost(design)
        cost += self.gamma * self.skewCost(design)
        return cost / (self.alpha + self.beta + self.gamma)
        
    def networkCost(self, design) :
        cost, queries = self.partialNetworkCost(design, self.workload)
        return cost
        
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
        sum_of_query_counts = 0
        sum_intervals = 0
        for i in range(0, len(segments)) :
            skew = 1 - segment_costs[i][0]
            sum_intervals += skew * segment_costs[i][1]
            sum_of_query_counts += segment_costs[i][1]
        return sum_intervals / sum_of_query_counts
        
    def partialNetworkCost(self, design, wrkld_sgmnt) :
        worst_case = 0
        result = 0
        stat_collections = list(self.stats)
        query_count = 0
        for s in wrkld_sgmnt.sessions :
            for q in s.queries :
                # Check to see if the queried collection exists in the design's 
                # denormalization scheme
                if design.hasCollection(q.collection) :
                    worst_case += self.nodes
                    query_count += 1
                    if q.type == 'insert' :
                        # is this assumption valid that an insert query would only make
                        # one request??
                        result += 1
                    else :
                        # Network costs of SELECT, UPDATE, DELETE queries are based off
                        # of using the sharding key in the predicate
                        if len(q.predicates) > 0 :
                            scan = True
                            query_type = None
                            for k,v in q.predicates.iteritems() :
                                if design.shardKeys[q.collection] == k :
                                    scan = False
                                    query_type = v
                            if scan == False :
                                # Query uses shard key... need to determine if this is an
                                # equality predicate or a range type
                                if v == 'equality' :
                                    result += 0.0
                                else :
                                    result += self.guessNodes(design, q.collection, k)
                            else :
                                result += self.nodes
                        else :
                            result += self.nodes
                else :
                    # Is this an incomplete design or has the collection been accounted
                    # for via denormalization... either way it will affect the 
                    # overall network cost
                    results += 0
        if worst_case == 0 :
            cost = 0
        else :
            cost = result / worst_case
        return (cost, query_count)
        
    '''
    Serve as a stand-in for the EXPLAIN function referenced in the paper?
    
    How do we use the statistics to determine the selectivity of this particular
    attribute and thus determine the number of nodes required to answer the query?
    '''
    def guessNodes(self, design, collection, key) : 
        return math.ceil(self.stats[collection]['fields'][key]['selectivity'] * self.nodes)
        
## CLASS
    