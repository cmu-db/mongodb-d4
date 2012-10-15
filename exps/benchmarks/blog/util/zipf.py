# -*- coding: utf-8 -*-

import math
import random
import bisect
import numpy as np


## -----------------------------------------------------
## Zipfian Distribution Generator
## -----------------------------------------------------
class ZipfGenerator: 
    
    def __init__(self, n, alpha = 1.001): 
        
        self.ndarray=np.random.zipf(alpha, n)
        self.currentpos = 0;
        self.num = n;
        # Calculate Zeta values from 1 to n: 
        #tmp = [1. / (math.pow(float(i), alpha)) for i in range(1, n+1)] 
        #zeta = reduce(lambda sums, x: sums + [sums[-1] + x], tmp, [0]) 
        
        
        
        # Store the translation map: 
        #self.distMap = [x / zeta[-1] for x in zeta] 
    
    def next(self): 
        
        if currentpos < self.num:
            tobereturned = ndarray[self.currentpos]
            self.currentpos += 1;
            return ndarray[self.currentpos]
        # Take a uniform 0-1 pseudo-random value: 
        # u = random.random()  
        
        # Translate the Zipf variable: 
        #return bisect.bisect(self.distMap, u) - 1

        #return i - 1 
## CLASS