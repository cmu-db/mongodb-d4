# -*- coding: utf-8 -*-

from pprint import pformat

## ==============================================
## DesignCandidates
## ==============================================
'''
An instance of this class is given to the BBSearch.
It basically defines the search space, i.e. BBSearch enumerates
possible solutions using this object.

= Basic structure of this class: =
set of COLLECTIONS mapped to:
a) list of possible shard keys
b) list of collections it can be denormalized to
c) list of possible index keys (this will be very likely the same as a))
'''
class DesignCandidates():

    '''
    class constructor
    '''
    def __init__(self):
        # collection names
        self.collections = set()
        # col names mapped to possible index keys
        self.indexKeys = {}
        # col names mapped to possible shard keys
        self.shardKeys = {}
        # col names mapped to possible col names the collection can be denormalized to
        self.denorm = {}
    

    def addCollection(self, collection, indexKeys, shardKeys, denorm) :
        if collection not in self.collections :
            self.collections.add(collection)
            self.indexKeys[collection] = indexKeys
            self.shardKeys[collection] = shardKeys
            self.denorm[collection] = denorm
    
    def getCandidates(self, collection_names):
        candidates = DesignCandidates()
        for coll_name in collection_names:
            candidates.addCollection(coll_name, self.indexKeys[coll_name], self.shardKeys[coll_name], self.denorm[coll_name])

        return candidates

    def __str__(self):
        return pformat(self.__dict__)

## CLASS