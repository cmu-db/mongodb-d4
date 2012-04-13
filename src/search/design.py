# -*- coding: utf-8 -*-

from util import *

## ==============================================
## Design
## ==============================================
class Design(object):

    def __init__(self):
        # set of collection names
        self.collections = []
        self.fields = {} # I think this does not need to be here
        self.shardKeys = {} 
        self.indexes = {}
        self.denorm = {}
    '''
    public methods
    '''
    # returns True when all collections are assigned
    def isComplete(self, totalNumberOfCollections):
        return len(self.collections) == totalNumberOfCollections
        
    
    def addCollection(self, collection) :
        if collection not in self.collections :
            self.collections.append(collection)
            self.indexes[collection] = [] # no indexes
            self.denorm[collection] = None # not denormalized
            self.shardKeys[collection] = None # no sharding
    
    def addCollections(self, collections) :
        for collection in collections :
            self.addCollection(collection)
    
    def removeCollection(self, collection):
        if collection not in self.collections:
            raise LookupError("Collection not found: " + collection)
        self.collections.remove(collection)
        self.shardKeys.pop(collection)
        self.indexes.pop(collection)
        self.denorm.pop(collection)
    
    
    def copy(self):
        d = Design()
        for c in self.collections:
            d.collections.append(c)
        for k in self.indexes.keys():
            d.indexes[k] = self.indexes[k]
        for k in self.shardKeys.keys():
            d.shardKeys[k] = self.shardKeys[k]
        for k in self.denorm.keys():
            d.denorm[k] = self.denorm[k]
        return d
            
    
    
    def addFieldsOneCollection(self, collection, fields) :
        self.fields[collection] = fields
    
    def addFields(self, fields) :
        self.fields = fields
    
    def addShardKey(self, collection, key) :
        self.shardKeys[collection] = key
    
    def addShardKeys(self, keys) :
        for k, v in keys.iteritems() :
            self.shardKeys[k] = v
    
    def addIndex(self, collection, index) :
        add = True
        for i in self.indexes[collection] :
            if i == index :
                add = False
        if add == True :
            self.indexes[collection].append(index)
    
    def addIndexes(self, indexes) :
        for k, v in indexes.iteritems() :
            for i in v :
                self.addIndex(k, i)
    
    
    def __str__(self):
        s=""
        for col in self.collections:
            s += " COLLECTION: " + col
            s += " indexes: " + str(self.indexes[col])
            s += " shardKey: " + str(self.shardKeys[col])
            s += " denorm: " + str(self.denorm[col]) + "\n"
        return s
            
    
    @staticmethod
    def testFactory() :
        design = Design()
        collections = ['col 1', 'col 2']
        design.addCollections(collections)
        design.addFieldsOneCollection('col 1', ['c1a', 'c1b', 'c1c', 'c1d'])
        design.addFieldsOneCollection('col 2', ['c2a', 'c2b', 'c2c', 'c2d'])
        design.addShardKey('col 1', 'c1b')
        design.addShardKey('col 2', 'c2a')
        design.addIndexes({ 'col 1' : [['c1a']], 'col 2' : [['c2c'], ['c2a', 'c2d']] })
        return design
## CLASS