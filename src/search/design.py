# -*- coding: utf-8 -*-

from util import *

## ==============================================
## Design
## ==============================================
class Design(object):

    def __init__(self):
        self.collections = []
        self.fields = {}
        self.shardKeys = {}
        self.indexes = {}
    
    def addCollection(self, collection) :
        if collection not in self.collections :
            self.collections.append(collection)
            self.indexes[collection] = []
    
    def addCollections(self, collections) :
        for collection in collections :
            self.addCollection(collection)
    
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
    
    @staticmethod
    def testDesignFactory() :
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