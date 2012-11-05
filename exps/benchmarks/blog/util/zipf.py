# -*- coding: utf-8 -*-

import math
import random
import bisect
import numpy as np


## -----------------------------------------------------
## Zipfian Distribution Generator
## -----------------------------------------------------
class ZipfGenerator: 
    
    def __init__(self, n, skewin = 0.8):  
        #if alpha <= 1.000:
        #    self.alph = 1.001
        #else: 
        #    self.alph = alpha
        self.skew = skewin
        self.num = n #expected returned numbers 0...31 (e.g for n=32 authors)
    
    def next(self): 
        #while 1:
        #    tobereturned = np.random.zipf(self.alph)
        #    if tobereturned  <= self.num:
        #        break
        #return tobereturned - 1;
        randomnum = random.random()
        if self.skew == 1.0:
            return 0
        if randomnum >= (1-self.skew): #80% of 
            selected = random.randrange(1, int((1-self.skew)*self.num))
            returnnum = selected * (1/(1-self.skew))
            #print("80%=>"+str(int((1-self.skew)*self.num)))
        elif randomnum < (1-self.skew): #20% of times
            selected = random.randrange(1,int(self.skew*self.num))
            returnnum = selected * (1/self.skew)
            #print("20%=>"+str(int(self.skew*self.num)))
        return int(returnnum-1)
## CLASS

