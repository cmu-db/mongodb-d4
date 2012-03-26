# -*- coding: utf-8 -*-

from util import *
import time

## ==============================================
## Branch and Bound search
## ==============================================


class BBSearch ():
    
    '''
    public methods
    '''
    
    def solve(self):
        self.optimal_solution = None
        self.startTime = time.time()
        # set initial bound to infinity
        self.bound = float("inf")
        self.rootNode.solve()
        self.onTerminate()
        return self.optimal_solution
        
    # this would stop the search
    def terminate(self):
        self.terminated = True
        
    '''
    private methods
    '''
    # changes keys back from ints to original collection names
    def restoreKeys(self):
        if self.optimal_solution == None:
            return
        oprime = {}
        for k in self.optimal_solution.keys():
            oprime[self.keys_dict[k]] = self.optimal_solution[k]
        self.optimal_solution = oprime
        
    '''
    Events
    '''
    
    # this event gets called when the search backtracks
    def onBacktrack(self):
        self.totalBacktracks+=1
        if time.time() - self.startTime > self.timeout:
            self.terminate()
        
    # this event gets called when the algorithm terminates
    def onTerminate(self):
        self.endTime = time.time()
        self.restoreKeys() # change keys to collection names
        print "\nSearch ended.\n"
        print "best solution: ", self.bound
        print "total backtracks: ", self.totalBacktracks
        print "time elapsed: ", self.endTime - self.startTime
        print "\nbest solution:\n", self.optimal_solution
    
    '''
    class constructor
    input: 
    initial design (type bbdesign)
    bounding function bf: f(bbdesign) --> float(0..1)
    timeout (in ms)
    '''
    def __init__(self, design, bf, to):
        # all nodes have a pointer to the bbsearch object
        # in order to access bounding function, optimial solution and current bound
        self.terminated = False
        self.keys_dict = design.transformKeys() #change keys from collection names to ints
        self.rootNode = BBNode(design, self, True) #rootNode: True
        self.bounding_function = bf
        self.totalBacktracks = 0
        self.timeout = to
        return
    
## CLASS



## ==============================================
## design - representation of a subsearch space or a possible assignment
## ==============================================
class BBDesign():
    
    # call this ONLY on the root node
    # to set all fields (collections) to None
    def initializeAssignment(self):
        self.assignment = {}
        for k in self.collections.keys():
            self.assignment[k] = None
    
    '''
    LEGACY CODE
    def getChildren(self):
        # find an unassigned field (i.e. None)
        for k in self.assignment.keys():
            if self.assignment[k] is None:
                # genereate possible designs according to the range in fields 
                result = []
                for v in self.collections[k]:
                    child = self.bbdesign(fields)
                    # inherit the parent assignment
                    child.assignment = self.assignment
                    # set the unassigned field to the one possible value
                    child.assignment[k] = v
                    result.append(child)
                ### for
                
                # return list of children
                return result
                
            ### if
        ### for
    '''
    
    # changes keys from colletion names to integeres
    # call only once (on root node)
    def transformKeys(self):
        cprime = {}
        keys_dict = {}
        i = 0
        for k in self.collections.keys():
            cprime[i] = self.collections[k]
            i+=1
        self.collections = cprime
        return keys_dict
    
    def __str__(self):
        return str(self.collections)
    
    # returns None if all children have been enumerated
    def getNextChild(self):
        #print self.assignment
        # determine which collections is to be assigned. Lazily, as everything else.
        if self.currentCol == None:
            for k in self.assignment.keys():
                if self.assignment[k] == None:
                    self.currentCol = k
                    break
            if self.currentCol == None:
                return None # all collections already assigned, no children needed
               
        # increment iterators
        self.currentDenorm += 1
        if self.currentDenorm == len(self.collections) - 1:
            self.currentDenorm = -1
            self.currentShardKey += 1
        if self.currentShardKey == len(self.collections[self.currentCol]):
            return None # all possible combinations have been tried
        
         
        # make the child
        child = BBDesign(self.collections)
        # inherit the parent assignment
        child.assignment = self.assignment
        # set the unassigned field to the one possible value
        shardKey = None
        if len(self.collections[self.currentCol]) > 0:
            shardKey = self.collections[self.currentCol][self.currentShardKey]
        denorm = None #not denormalized
        if (self.currentDenorm) > -1:
            denorm = self.currentDenorm
        child.assignment[self.currentCol] = (shardKey, denorm)
        
        print "created child: ", child.assignment
        
        return child
        
    '''
    class constructor
     input: collections c - collections are basically fields we want to assign values to in the bb search
     each collection maps to a list of [possible_sharding_keys]
     example of input c: 
          {'col1': ['id', 'timestamp', 'author'], 'col2': [], 'col3': ['title', 'date']}
     assignment: a (possibly incomplete) solution
     assignment example:
       {'col1': ('id', None), 'col2': (None, None), 'col3': (None, 2), 'col4': None}
     (col1, col2 are assigend possible values, col3 got denormalized to col2, col4 still unassigned)
    '''
    def __init__(self, c):
        self.collections = c
        # self.assignment gets initialized either in initializeAssignment (for ROOT node only)
        # or when enumerating children
        
        # iterators to generate children
        self.currentCol = None
        self.currentShardKey = 0
        self.currentDenorm = -2 #first value will be -1, which is 'not denormalized'
        return

### CLASS



## ==============================================
## bbnode: main building block of the BB search tree
## ==============================================
class BBNode():

   # this is depth first search for now
    def solve(self):
        self.populateChildren()    
        for child in self.children:
            child.solve()
            if self.bbsearch.terminated:
                return
            ## IF
            
            #child returned --> we backtracked
            self.bbsearch.onBacktrack()
        
        ## FOR
            
        
    def populateChildren(self):
        # branches the current design and populates the child node list
        d = self.design.getNextChild()
        while d is not None:
            childNode = BBNode(d, self.bbsearch, False) # root: False
            
            if childNode.evaluate():
                self.children.append(childNode)
            
            d = self.design.getNextChild()
        
        ## WHILE
            
    # computes the cost of this design
    # if the cost is < current bound, it updates the optimal solution
    # returns False if the cost is more than the bound --> don't include this node
    def evaluate(self):
        # add child only when the solution is admissible
        self.cost = self.bbsearch.bounding_function(self.design)
        if self.cost < self.bbsearch.bound:
            self.bbsearch.bound = self.cost
            self.bbsearch.optimal_solution = self.design.assignment
        
        return self.cost <= self.bbsearch.bound

    '''
    class constructor
     input:
     design d
     bbsearch object bb
     isroot - True/False
    '''
    def __init__(self, d, bb, isroot):
        self.design = d
        self.bbsearch = bb
        self.children = []
        if isroot:
            self.design.initializeAssignment()
        return
        

## CLASS