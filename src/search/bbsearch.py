# -*- coding: utf-8 -*-

from util import *
import time

## ==============================================
## Branch and Bound search
## ==============================================


class bbsearch(Object):
    
    ### public methods
    
    def solve():
        self.optimial_solution = None
        self.startTime = time.time()
        # set initial bound to infinity
        self.bound = float("inf")
        rootNode.solve()
        onTerminate()
        return optimal_solution
        
    # this would stop the search
    def terminate():
        self.terminated = True
        
    ### Events    
    
    # this event gets called when the search backtracks
    def onBacktrack():
        totalBacktracks++
        if time.time() - startTime > timeout:
            terminate()
        
    # this event gets called when the algorithm terminates
    def onTerminate():
        self.endTime = time.time()
        print "\nSearch ended.\n"
        print "best solution: ", bound
        print "total backtracks: ", totalBacktracks
        print "time elapsed: ", endTime - startTime
        print "\nbest solution:\n", optimial_solution
    
    # input: initial bbdesign
    # bounding function bf
    # timeout to in ms
    def __init__(self, design, bf, to):
        # all nodes have a pointer to the bbsearch object
        # in order to access bounding function, optimial solution and current bound
        self.terminated = False
        self.rootNode = bbnode(design, self)
        self.bounding_function = bf
        self.totalBacktracks = 0
        self.timeout = to
        return
    
## CLASS



## ==============================================
## design - representation of a subsearch space or a possible assignment
## ==============================================
class bbdesign(Object):
    
    # call this ONLY on the root node
    # to set all fields (collections) to None
    def initializeAssignment():
        self.assignment = fields
        for k in fields.keys():
            assignment[k] = None
    
    def getChildren():
        # find an unassigned field (i.e. None)
        for k in assignment.keys():
            if assignment[k] is None:
                # genereate possible designs according to the range in fields 
                result = []
                for v in fields[k]:
                    child = bbdesign(fields)
                    # inherit the parent assignment
                    child.assignment = assignment
                    # set the unassigned field to the one possible value
                    child.assignment[k] = v
                    result.append(child)
                ### for
                
                # return list of children
                return result
                
            ### if
        ### for
    
    # returns None if all children have been enumerated
    def getNextChild():
        currentDenorm++
        if currentDenorm == len(collections) - 1:
            currentDenorm = -1
            currentShardKey++
        if currentShardKey == len(collections[currentCol]):
            currentShardKey = 0
            currentCol++
        if currentCol == len(collections.keys():
            return None
        
            
                    
    # input: collections c - collections are basically fields we want to assign values to in the bb search
    # each collection maps to a list of [possible_sharding_keys]
    # example of input c: 
    #      {'col1': ['id', 'timestamp', 'author'], 'col2': [], 'col3': ['title', 'date']}
    # assignment: a (possibly incomplete) solution
    # assignment example:
    #   {'col1': ('id', None), 'col2': (None, None), 'col3': (None, 2), 'col4': None}
    # (col1, col2 are assigend possible values, col3 got denormalized to col2, col4 still unassigned)
    def __init__(self, c):
        self.collections = c
        # self.assignment gets initialized either in initializeAssignment (for ROOT node only)
        # or when enumerating children
        
        # iterators to generate children
        self.currentCol = -1
        self.currentShardKey = 0
        self.currentDenorm = -2 #first value will be -1, which is 'not denormalized'
        return

### CLASS



## ==============================================
## bbnode: main building block of the BB search tree
## ==============================================
class bbnode(Object):

   # this is depth first search for now
    def solve():
        populateChildren()    
        for child in children:
            child.solve()
            if terminated:
                return
            #child returned --> we backtracked
            bbsearch.onBacktrack()
            
        
    def populateChildren():
        # branches the current design and populates the child node list
        for d in design.getChildren():
            childNode = bbnode(design, bounding_function, bbsearch)
            
            if childNode.evaluate():
                children.append(childNode)
            
    # computes the cost of this design
    # if the cost is < current bound, it updates the optimal solution
    # returns False if the cost is more than the bound --> don't include this node
    def evaluate():
        # add child only when the solution is admissible
        self.cost = bbsearch.bounding_function(design)
        if cost < bbsearch.bound:
            bbsearch.bound = cost
            bbsearch.optimal_solution = design.assignment
        
        return cost <= bbsearch.bound

    # input:
    # design d
    # bounding function c
    # bbsearch object bb
    def __init__(self, d, c, bb):
        self.bounding_function = c
        self.design = d
        self.bbsearch = bb
        self.children = []
        return
        

## CLASS