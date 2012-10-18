# -*- coding: utf-8 -*-

import logging
import json
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## Design
## ==============================================
class Design(object):

    def __init__(self):
        self.data = {}
    # DEF

    def reset(self, collectionName):
        self.data[collectionName] = None

    def isRelaxed(self, col_name):
        return self.data[col_name] is None
    
    def recover(self, col_name):
        self.data[col_name] = {
            'indexes' : [],
            'shardKeys' : [],
            'denorm' : None
        }
    def isComplete(self):
        """returns True when all collections are assigned designs"""
        for value in self.data.values():
            if value is None:
                return False

        return True
    ## DEF
    
    def getCollections(self):
        return self.data.keys()
    ## DEF
    
    def addCollection(self, col_name):
        assert not col_name in self.data, \
            "Trying to add collection '%s' more than once" % col_name
        self.data[col_name] = {
            'indexes' : [],
            'shardKeys' : [],
            'denorm' : None
        }
    ## DEF

#    @DeprecationWarning
    def addCollections(self, collections):
        for collection in collections :
            self.addCollection(collection)
    ## DEF

    def hasCollection(self, col_name):
        return col_name in self.data
    ## DEF
    
        '''
    @todo: re-implement
    '''
    def copy(self):
        d = Design()
        for k,v in self.data.iteritems():
            d.addCollection(k)
            if v is None:
                d.reset(k)
            else:
                d.addShardKey(k, self.getShardKeys(k))
                d.setDenormalizationParent(k, self.getDenormalizationParent(k))
                indexes = self.getIndexes(k)
                if indexes:
                    for i in indexes :
                        d.addIndex(k, i)
        return d

    ## ----------------------------------------------
    ## COMPARISON METHODS
    ## ----------------------------------------------
        
    def getDelta(self, other):
        """
            Return the list of collection names that have a different design
            configuration in this design than in the one provided.
            If a collection that is in this design is missing in other, then
            that will count as a difference
        """
        if other is None:
            return self.data.keys()
        
        result = [ ]
        for col_name in self.data.iterkeys():
            match = True
            if not other or not col_name in other.data:
                match = False
            else:
                if self.data[col_name] and other.data[col_name]:
                    for k, v in self.data[col_name].iteritems():
                        if v <> other.data[col_name].get(k, None):
                            match = False
                            break
                else:
                    match = False
                    
            if not match: result.append(col_name)
        ## FOR
        return result
    ## DEF

    def hasDenormalizationChanged(self, other, col_name):
        """
            Returns true if the denormalization scheme has changed in
            the other design for the given collection name
        """
        if other is None: return False
        if not col_name in self.data:
            return (col_name in other)
        
        if not self.data[col_name] or not other.data[col_name]:
            return False
        
        return self.data[col_name]['denorm'] != other.data[col_name]['denorm']
    ## DEF
    
    def hasShardingKeysChanged(self, other, col_name):
        """
            Returns true if the sharding keys have changed in the other design
            for the given collection name
        """
        if other is None: return False
        if not col_name in self.data:
            return (col_name in other)
        
        if not self.data[col_name] or not other.data[col_name]:
            return False
        
        return self.data[col_name]['shardKeys'] != other.data[col_name]['shardKeys']
    ## DEF

    ## ----------------------------------------------
    ## DENORMALIZATION
    ## ----------------------------------------------
        
    def isDenormalized(self, col_name):
        return not self.getDenormalizationParent(col_name) is None
    ## DEF
    
    def setDenormalizationParent(self, col_name, parent):
        self.data[col_name]['denorm'] = parent
    ## DEF
    
    def getDenormalizationParent(self, col_name):
        if col_name in self.data and \
            self.data[col_name] and \
            self.data[col_name]['denorm'] and \
            self.data[col_name]['denorm'] != col_name:
            return self.data[col_name]['denorm']
        return None
    ## DEF
    
    def getDenormalizationHierarchy(self, col_name, ret=None):
        if not ret: ret = [ ]
        parent = self.getDenormalizationParent(col_name)
        if parent:
            ret.insert(0, parent) 
            return self.getDenormalizationHierarchy(parent, ret)
        return ret
    ## DEF
            
    def getParentCollection(self, col_name):
        if col_name in self.data:
            if not self.data[col_name]['denorm'] :
                return None
            else :
                return self.getParentCollection(self.data[col_name]['denorm'])
        else :
            return None
    ## DEF

    ## ----------------------------------------------
    ## SHARD KEYS
    ## ----------------------------------------------
        
    def addShardKey(self, col_name, key):
        if key:
            self.data[col_name]['shardKeys'] = key
    ## DEF

    def getShardKeys(self, col_name):
        if self.data[col_name]:
            return self.data[col_name]['shardKeys']
    ## DEF
    
    def getAllShardKeys(self):
        keys = {}
        for k, v in self.data.iteritems():
            keys[k] = v['shardKeys']
        return keys
    ## DEF
    
    def addShardKeys(self, keys):
        if keys:
            for k, v in keys.iteritems():
                self.data[k]['shardKeys'] = v
    ## DEF

    def inShardKeyPattern(self, col_name, attr):
        return attr in self.data[col_name]['shardKeys']
    ## DEF

    ## ----------------------------------------------
    ## INDEXES
    ## ----------------------------------------------
        
    def getIndexes(self, col_name):
        if self.data[col_name]:
            return self.data[col_name]['indexes']
    ## DEF

    def getAllIndexes(self):
        return dict(self.data.iteritems())
    ## DEF

    def addIndex(self, col_name, indexKeys):
        if indexKeys:
            if not type(indexKeys) == tuple:
                indexKeys = tuple(indexKeys)
            add = True
            for i in self.data[col_name]['indexes'] :
                if i == indexKeys:
                    add = False
                    break
            if add:
                LOG.debug("Adding index '%s/%s' for collection %s", \
                          indexKeys, type(indexKeys), col_name)
                self.data[col_name]['indexes'].append(indexKeys)
    ## DEF
    
    def hasIndex(self, col_name, list):
        if self.data[col_name]:
            return False

        for field in list :
           for i in self.data[col_name]['indexes'] :
               if field in i :
                   return True
        return False
    ## DEF


    ## ----------------------------------------------
    ## UTILITY CODE
    ## ----------------------------------------------

    def __str__(self):
        ret = ""
        ctr = 0
        LOG.debug("Data\n%s", self.data)
        for col_name in sorted(self.data.iterkeys()):
            ret += "[%02d] %s\n" % (ctr, col_name)
            if self.data[col_name]:
                for k, v in self.data[col_name].iteritems():
                    ret += "  %-10s %s\n" % (k+":", v)
            ctr += 1
        return ret
    ## DEF

    def toJSON(self):
        return json.dumps(self.toDICT(), sort_keys=False, indent=4)

    def toDICT(self):
        return self.data
    ## DEF

## CLASS