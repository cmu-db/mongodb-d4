# -*- coding: utf-8 -*-

from util import *
import json

## ==============================================
## Design
## ==============================================
class Design(object):

    def __init__(self):
        self.data = {}
        
    '''
    public methods
    '''
    # returns True when all collections are assigned
    def isComplete(self, totalNumberOfCollections):
        return len(self.data) == totalNumberOfCollections
    ## DEF
    
    def getCollections(self):
        return list(self.data)
    ## DEF
    
    def addCollection(self, collection) :
        if collection not in list(self.data) :
            self.data[collection] = {
                'indexes' : [],
                'shardKeys' : [],
                'denorm' : None
            }
    ## DEF
    
    def addCollections(self, collections) :
        for collection in collections :
            self.addCollection(collection)
    ## DEF
    
    def removeCollection(self, collection):
        if collection not in list(self.data) :
            raise LookupError("Collection not found: " + collection)
        self.data.pop(collection)
	## DEF
	    
    def hasCollection(self, collection) :
        return collection in list(self.data)
    ## DEF
    
    '''
    @todo: re-implement
    '''
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
          
          
    def isDenormalized(self, collection):
        return self.getDenormalizationParent(collection) != None
    ## DEF
    
    def setDenormalizationParent(self, collection, parent):
        self.data[collection]['denorm'] = parent
    ## DEF
    
    def getDenormalizationParent(self, collection):
        if collection in list(self.data) and \
           self.data[collection]['denorm'] and \
           self.data[collection]['denorm'] != collection:
            return self.data[collection]['denorm']
        return None
    ## DEF
    
    def getDenormalizationHierarchy(self, collection, ret=None):
        if ret == None: ret = [ ]
        parent = self.getDenormalizationParent(collection)
        if parent:
            ret.insert(0, parent) 
            return self.getDenormalizationHierarchy(parent, ret)
        return ret
    ## DEF
            
    def getParentCollection(self, collection) :
        if collection in list(self.data):
            if not self.data[collection]['denorm'] :
                return collection
            else :
                return self.getParentCollection(self.data[collection]['denorm'])
        else :
            return None
    ## DEF
    
    def addShardKey(self, collection, key) :
        self.data[collection]['shardKeys'] = key
    ## DEF

    def getShardKey(self, collection) :
        return self.data[collection]['shardKeys']
    ## DEF
    
    def getShardKeys(self) :
        keys = {}
        for k, v in self.data.iteritems() :
            keys[k] = v['shardKeys']
        return keys
    ## DEF
    
    def addShardKeys(self, keys) :
        for k, v in keys.iteritems() :
            self.data[k]['shardKeys'] = v
    ## DEF
    
    def addIndex(self, collection, index) :
        add = True
        for i in self.data[collection]['indexes'] :
            if i == index :
                add = False
        if add == True :
            self.data[collection]['indexes'].append(index)
    ## DEF
    
    def addIndexes(self, indexes) :
        for k, v in indexes.iteritems() :
            for i in v :
                self.addIndex(k, i)
    ## DEF
    
    def hasIndex(self, collection, list) :
        for field in list :
           for i in self.data[collection]['indexes'] :
               if field in i :
                   return True
        return False
    ## DEF
    
    def __str__(self):
        s=""
        for k, v in self.data.iteritems() :
            s += " COLLECTION: " + k
            s += " indexes: " + str(v['indexes'])
            s += " shardKey: " + str(v['shardKeys'])
            s += " denorm: " + str(v['denorm']) + "\n"
        return s
    ## DEF
            
    def toJSON(self) :
        return json.dumps(self.toDICT(), sort_keys=False, indent=4)
    
    def toDICT(self) :
        return self.data
    ## DEF
    
    def inShardKeyPattern(self, collection, attr) :
        return attr in self.data[collection]['shardKeys']
    ## DEF
    
    def getIndexesForCollection(self, collection) :
        return self.data[collection]['indexes']
    ## DEF 
    
    def getIndexes(self) :
        indexes = {}
        for k,v in self.data.iteritems() :
            indexes[k] = v['indexes']
        return indexes
    ## DEF
    
    @staticmethod
    def testFactory() :
        design = Design()
        collections = ['col 1', 'col 2']
        design.addCollections(collections)
        design.addShardKey('col 1', ['c1b'])
        design.addShardKey('col 2', ['c2a'])
        design.addIndexes({ 'col 1' : [['c1a']], 'col 2' : [['c2c'], ['c2a', 'c2d']] })
        return design
## CLASS