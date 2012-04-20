# -*- coding: utf-8 -*-
from __future__ import division
import sys
import json
import logging
import math
import random

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
        self.rg = random.Random()
        self.rg.seed('cost model coolness')
        # Convert GB to KB
        self.max_memory = config['max_memory'] * 1024 * 1024
        # Size of an index per document (default 10 kb)
        if 'index_node_size' in list(config) :
            self.index_node_size = config['index_node_size']
        else :
            self.index_node_size = 1
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
    ## end def ##
    
    def diskCost(self, design):
        # 1. estimate index memory requirements
        index_memory = self.getIndexSize(design)
        if index_memory > self.max_memory :
            return 10000000000000
        
        # 2. approximate distribution of working sets based on the
        #    frequency with which collections are queried in the working set
        working_set = self.estimateWorkingSets(design, self.max_memory - index_memory)
        
        # 3. Iterate of workload, foreach query:
        # a. how many page reads will be required to satisfy the data
        for s in self.workload.sessions :
            for q in s.queries :
                # is the collection in the design - if not ignore
                if design.hasCollection(q.collection) == False :
                    break
                    
                # Does this depend on the type of query? (insert vs update vs delete vs select)
                # 1. is there an index on a predicate
                # 2. predict if data is in working set
                ws_hit = self.rg.randint(1, 100)
                if ws_hit <= working_set[q.collection] :
                    print 'Working Set Hit !!!!!!'
                else :
                    print 'Working Set Miss :('
                pass
        return 1.0
    ## end def ##
    
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
        
        if sum_of_query_counts == 0 :
            return 0
        else :
            return sum_intervals / sum_of_query_counts
        
    def partialNetworkCost(self, design, wrkld_sgmnt) :
        worst_case = 0
        result = 0
        stat_collections = list(self.stats)
        query_count = 0
        for s in wrkld_sgmnt.sessions :
            previous_query = None
            for q in s.queries :
                # Check to see if the queried collection exists in the design's 
                # de-normalization scheme
                if design.hasCollection(q.collection) :
                    process = False
                    parent_col = design.getParentCollection(q.collection)
                    if previous_query == None :
                        process = True
                    elif parent_col == q.collection :
                        process = True
                    elif previous_query.type <> 'select' or q.type <> 'select' :
                        process = True
                    elif previous_query.collection <> parent_col :
                        process = True
                    if process == True :
                        worst_case += self.nodes
                        query_count += 1
                        if q.type == 'insert' :
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
                        # query does not need to be processed
                        pass
                else :
                    # Collection is not in design.. don't count query
                    pass
                previous_query = q
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
        
    '''
    Estimate the amount of memory required by the indexes of a given design
    '''
    def getIndexSize(self, design) :
        memory = 0
        for col in design.collections :
            # Add a hit for the index on '_id' attribute for each collection
            memory += self.stats[col]['tuple_count'] * self.index_node_size
            
            # Process other indexes for this collection in the design
            memory += self.stats[col]['tuple_count'] * self.index_node_size * len(design.indexes[col])
        return memory
        
    '''
    Estimate the number of documents per collection that will fit in working set space
    '''
    def estimateWorkingSets(self, design, capacity) :
        working_set_counts = {}
        for col in design.collections :
            working_set_counts[col] = capacity * self.stats[col]['workload_percent']
            working_set_counts[col] = working_set_counts[col] / self.stats[col]['kb_per_doc']
            working_set_counts[col] = working_set_counts[col] / self.stats[col]['tuple_count']
            working_set_counts[col] = math.ceil(working_set_counts[col] * 100)
        return working_set_counts
## CLASS
    