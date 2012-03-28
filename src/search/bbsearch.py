# -*- coding: utf-8 -*-

from util import *
import time
import sys

## ==============================================
## Branch and Bound search
## ==============================================


'''
Usage:
1) Instantiate BBSearch object. The constructor takes these args:
* collections:  dictionary mapping collection names to possible shard keys
                example: collections = {"col1": ["shardkey1", "shardkey2"], "col2": ["id"], "col3": ["id"]}
* bounding function: takes BBDesign object, and returns float 0..1
* timeout (in sec)

2) call solve()
That's it.
'''

class BBSearch ():
    
    # bbsearch object has self.status field, which can have following values:
    # initialized, solving, solved, timed_out, user_terminated
    
    '''
    public methods
    '''
    # main public method. Simply call to get the optimal solution
    def solve(self):
        self.status = "solving"
        print("===BBSearch Solve===")
        print " timeout: ", self.timeout
        self.optimal_solution = None
        self.startTime = time.time()
        # set initial bound to infinity
        self.upper_bound = float("inf")
        self.lower_bound = float("inf")
        self.rootNode.solve()
        if self.status is "solving":
            self.status = "solved"
        self.onTerminate()
        return self.optimal_solution
       
       
    # traverses the entire tree and returns nodes as list
    # mostly for testing
    # must solve first. Returns only childNodes visited while solving
    def listAllNodes(self):
        result = [self.rootNode]
        self.rootNode.addChildrenToList(result)
        return result
       
    # only called externally. This stops the search.
    def terminate(self):
        self.status = "user_terminated"
        self.terminated = True
        
    '''
    private methods
    '''
    
    def checkTimeout(self):
        if time.time() - self.startTime > self.timeout:
            self.status = "timed_out"
            self.terminated = True
    
    '''
    Events
    '''
    
    # this event gets called when the search backtracks
    def onBacktrack(self):
        self.totalBacktracks += 1
        self.checkTimeout()
        
    # this event gets called when the algorithm terminates
    def onTerminate(self):
        self.endTime = time.time()
        #self.restoreKeys() # change keys to collection names
        print "\n===Search ended==="
        print "status: ", self.status
        print "upper bound: ", self.upper_bound
        print "lower bound: ", self.lower_bound
        print "total backtracks: ", self.totalBacktracks
        print "time elapsed: ", self.endTime - self.startTime
        print "best solution:\n", self.optimal_solution
        print "\n------------------"
    
    '''
    class constructor
    input: 
    collections: dict mapping collection names to possible sharding keys
    bounding function bf: f(bbdesign) --> float(0..1)
    timeout (in sec)
    '''
    def __init__(self, collections, bf, to):
        # all nodes have a pointer to the bbsearch object
        # in order to access bounding function, optimial solution and current bound
        self.terminated = False
        # store keys list... used only to translate integer iterators back to real key values...
        design = BBDesign(collections)
        self.keys_list = design.collections.keys()
        self.rootNode = BBNode(design, self, True, 0) #rootNode: True
        self.bounding_function = bf
        self.totalBacktracks = 0
        self.timeout = to
        self.status = "initialized"
        return
    
## CLASS



## ==============================================
## BBDesign - representation of an (incomplete) solution. Embedded in BBNode
## ==============================================
'''
NOTE: BBDesign is the input for our COST FUNCTION.
All we care about is BBDesign.assignment, which is just a dictionary
mapping collections to tuples (shardKey, denorm),
where shardKey is obviously the field we'd shard on (or None)
and denorm is either None (no embedding) or the name of the enclosing collection 

A collection in BBDesign.assignment may map to None, which means
this assignment/solution is incomplete, i.e. this collection has not been assigned yet.

Example of BBDesign.assignment:
assigment: {'col2': ('id', 'col3'), 'col3': ('id', None), 'col1': None}
'''
class BBDesign():
    
    # call this ONLY on the root node
    # to set all fields (collections) to None
    def initializeAssignment(self):
        self.assignment = {}
        for k in self.collections.keys():
            self.assignment[k] = None
    
    def __str__(self):
        return str(self.assignment)
    
    # returns None if all children have been enumerated
    def getNextChild(self, keys_list):
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
        if self.currentDenorm == len(self.collections):
            self.currentDenorm = -1
            self.currentShardKey += 1
        if self.currentShardKey == len(self.collections[self.currentCol]):
            return None # all possible combinations have been tried
        
        # determine the value for this collection
        shardKey = None
        if (self.currentShardKey >= 0) and (len(self.collections[self.currentCol]) > 0):
            shardKey = self.collections[self.currentCol][self.currentShardKey]
        denorm = None #not denormalized
        if (self.currentDenorm) > -1:
            denorm = keys_list[self.currentDenorm]
        
        ### --- Solution Feasibility Check ---
        
        # IMPORTANT
        # This might be a stupid way of doing it, but let's go with it for now
        # --> check feasibility of this partial solution:
        #   * embedded collections should not have a sharding key
        #       -note: actually, the sharding key could be picked from the embedded collection,
        #       but in that case we must ensure the sharding key is not assigned on the enclosing collection...
        #   * NO CIRCULAR EMBEDDING
        
        feasible = True
        # no embedding in itself
        if denorm is not None:
            if denorm == self.currentCol:
                feasible = False
        # no circular embedding
        if denorm is not None:
            embedded_in = denorm
            if self.assignment[embedded_in] is not None:
                if self.assignment[embedded_in][1] == self.currentCol:
                    feasible = False # 'current_col' would be embedded in 'denorm' and vice versa...
        # enforce sharding keys...
        if (denorm is not None) and (shardKey is not None):
            # if denormalized and with a sharding key, make sure the embedding collection has no sharding key
            embedded_in = denorm
            if self.assignment[embedded_in] is not None:
                if self.assignment[embedded_in][0] is not None:
                    feasible = False
        
        # well, this is a very lazy way of doing it :D
        # it OK so long there are not too many consecutive infeasible nodes,
        # then it could hit the max recursion limit...
        if not feasible:
            return self.getNextChild(keys_list)
        
        ### --- end of Solution Feasibility Check ---
        
        
        # make the child
        child = BBDesign(self.collections)
        # inherit the parent assignment
        child.assignment = {} #must copy it
        for k in self.assignment.keys():
            child.assignment[k] = self.assignment[k]
        # set the unassigned field to the one possible value
        
        child.assignment[self.currentCol] = (shardKey, denorm)
        
        #print "created child: ", child.assignment
        
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
        self.currentShardKey = -1
        self.currentDenorm = -2 #first value will be -1, which is 'not denormalized'
        return

### CLASS



## ==============================================
## BBNode: main building block of the BBSearch tree
## ==============================================
class BBNode():

   # this is depth first search for now
    def solve(self):
        self.bbsearch.checkTimeout()
        if self.bbsearch.terminated:
            return
        self.populateChildren()    
        for child in self.children:
            child.solve()
            
            ## IF
            
            #child returned --> we backtracked
            self.bbsearch.onBacktrack()
            if self.bbsearch.terminated:
                return
        
        ## FOR
        
            
        
    def populateChildren(self):
        # branches the current design and populates the child node list
        d = self.design.getNextChild(self.bbsearch.keys_list)
        while d is not None:
            childNode = BBNode(d, self.bbsearch, False, self.depth+1) # root: False
            
            if childNode.evaluate():
                self.children.append(childNode)
            
            d = self.design.getNextChild(self.bbsearch.keys_list)
        
        ## WHILE
            
    # computes the cost of this design
    # if the cost is < current bound, it updates the optimal solution
    # returns False if the cost is more than the bound --> don't include this node
    def evaluate(self):
        print ".",
        #print self
        sys.stdout.flush()
        # add child only when the solution is admissible
        cost = self.bbsearch.bounding_function(self.design)
        self.lower_bound = cost[0]
        self.upper_bound = cost[1]
        #print "EVAL NODE: ", self.design, " bound_lower: ", self.lower_bound, "bound_upper: ", self.upper_bound, "BOUND: ", self.bbsearch.lower_bound
        if self.upper_bound < self.bbsearch.lower_bound:
            self.bbsearch.lower_bound = self.upper_bound
            self.bbsearch.optimal_solution = self.design.assignment
        
        if self.upper_bound <= self.bbsearch.upper_bound:
            self.bbsearch.upper_bound = self.upper_bound
        
        return self.lower_bound <= self.bbsearch.upper_bound
        #return True

    # mostly for testing. Recursive.
    def addChildrenToList(self, result):
        for c in self.children:
            result.append(c)
            c.addChildrenToList(result)

    def __str__(self):
        tab="\n"
        for i in range(self.depth):
            tab+="\t"
        s = tab+"--node--"\
        +tab+" lower bound: " + str(self.lower_bound)\
        +tab+" upper bound: " + str(self.upper_bound)\
        +tab+" assigment: " + str(self.design)\
        +tab+" children: " + str(len(self.children))\
        +tab+" depth: " + str(self.depth)
        return s

    '''
    class constructor
     input:
     design d
     bbsearch object bb
     isroot - True/False
     depth 
    '''
    def __init__(self, d, bb, isroot, depth):
        self.lower_bound = None
        self.upper_bound = None
        self.depth = depth
        self.design = d
        self.bbsearch = bb
        self.children = []
        if isroot:
            self.design.initializeAssignment()
        return
        

## CLASS