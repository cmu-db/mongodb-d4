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
        if alpha <= 1.000:
            self.alph = 1.001
        elif: 
            self.alph = alpha
        self.num = n #expected returned numbers 0...31 (e.g for n=32 authors)
    
    def next(self): 
        while 1:
            tobereturned = np.random.zipf(self.alph)
            if tobereturned  <= self.num:
                break
        return tobereturned - 1;
## CLASS

