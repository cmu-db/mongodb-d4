#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import logging
import time
import unittest
import itertools
from pprint import pformat 

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

from search import bbsearch
from util import constants
LOG = logging.getLogger(__name__)

# simple test enumerating all nodes of the search space
class TestCompoundKeyIterator (unittest.TestCase):
    
    def setUp(self):
        pass
    ## DEF

    def testIfWeFindAllInvalidCombinations(self):
        '''
            The compoundKeyIterator tries to get all the possible combinations of the given keys
            but it is proper to evaluate some combinations, like ((f0), (f0, f1)).
            We want to prune out these kinds of combinations on the fly
        '''
        singleCandidateKeys = [ ]
        for i in xrange(2):
            singleCandidateKeys.append("f" + str(i))

        candidateKeys = self.__calculate_permutations__(singleCandidateKeys)
        print "candidateKeys: ", len(candidateKeys)
        originalkeys = self.__calculate_combinations__(candidateKeys)
        print "originalkeys: ", len(originalkeys)
        iterator = bbsearch.CompoundKeyIterator(candidateKeys, -1)
        print "invalide keys: ", len(iterator.invalidCombinations)
        
        valid_keys = [ ]
        while True:
            try:
                res = None
                while res == None:
                    res = iterator.next()
                ## WHILE
                if len(res) != 0:
                    valid_keys.append(res)
                    self.assertTrue(res not in iterator.invalidCombinations)
            except StopIteration:
                break
        print "valid_keys: ", len(valid_keys)
        
        self.assertEqual(len(valid_keys) + len(iterator.invalidCombinations), len(originalkeys))
    ## DEF
    
    def __calculate_combinations__(self, keys, store=True):
        candidateKeys = []
        counter = 0
        for i in xrange(1, len(keys) + 1):
            if i > constants.MAX_INDEX_SIZE: break
            candidateKeysIter = itertools.combinations(keys, i)
            try:
                while True:
                    if store:
                        candidateKeys.append(candidateKeysIter.next())
                    else:
                        candidateKeysIter.next()
                        counter += 1
                ## WHILE
            except StopIteration:
                pass
        ## FOR 
        #print "number: ", counter
        return candidateKeys
    ## DEF
    
    def __calculate_permutations__(self, keys, store=True):
        candidateKeys = []
        counter = 0
        for i in xrange(1, len(keys) + 1):
            if i > constants.MAX_INDEX_SIZE: break
            candidateKeysIter = itertools.permutations(keys, i)
            try:
                while True:
                    if store:
                        candidateKeys.append(candidateKeysIter.next())
                    else:
                        candidateKeysIter.next()
                        counter += 1
                ## WHILE
            except StopIteration:
                pass
        ## FOR 
        #print "number: ", counter
        return candidateKeys
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()