# -*- coding: utf-8 -*-

from util import *


## ==============================================
## Branch and Bound search
## ==============================================


class bbsearch(Object):
    
    def solve():
        self.optimial_solution = None
        # set initial bound to infinity
        self.bound = float("inf")
        rootNode.solve()
        return optimal_solution
        
    
    # input: initial bbdesign
    # bounding function bf
    def __init__(self, design, bf):
        # all nodes have a pointer to the bbsearch object
        # in order to access bounding function, optimial solution and current bound
        self.rootNode = bbnode(design, self)
        self.bounding_function = bf
        return
    
## CLASS



## ==============================================
## design - representation of a subsearch space or a possible assignment
## ==============================================
class bbdesign(Object):
    
    # call this on the root node
    # sets all fields to None
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
                
                    
    #input: fields f - map of all fields to possible value,
    # i.e. {field1: [1..3], field2: [True, False], ...}
    def __init__(self, f):
        self.fields = f
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