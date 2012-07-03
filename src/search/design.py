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

    def isComplete(self, totalNumberOfCollections):
        """returns True when all collections are assigned"""
        return len(self.data) == totalNumberOfCollections
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
    
    def hasCollection(self, col_name) :
        return col_name in self.data
    ## DEF

    def getDelta(self, other):
        """
            Return the list of collection names that have a different design
            configuration in this design than in the one provided.
            If a collection that is in this design is missing in other, then
            that will count as a difference
        """
        result = [ ]
        for col_name in self.data.iterkeys():
            match = True
            if not other or not col_name in other.data:
                match = False
            else:
                for k, v in self.data[col_name].iteritems():
                    if v <> other.data[col_name].get(k, None):
                        match = False
                        break
            if not match: result.append(col_name)
        ## FOR
        return result
    ## DEF

    '''
    @todo: re-implement
    '''
    def copy(self):
        d = Design()
        for k,v in self.data.iteritems() :
            d.addCollection(k)
            d.addShardKey(k, self.getShardKeys(k))
            d.setDenormalizationParent(k, self.getDenormalizationParent(k))
            indexes = self.getIndexes(k)
            for i in indexes :
                d.addIndex(k, i)
        return d

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
            
    def getParentCollection(self, col_name) :
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

    def addShardKey(self, col_name, key) :
        self.data[col_name]['shardKeys'] = key
    ## DEF

    def getShardKeys(self, col_name) :
        return self.data[col_name]['shardKeys']
    ## DEF
    
    def getAllShardKeys(self) :
        keys = {}
        for k, v in self.data.iteritems() :
            keys[k] = v['shardKeys']
        return keys
    ## DEF
    
    def addShardKeys(self, keys) :
        for k, v in keys.iteritems() :
            self.data[k]['shardKeys'] = v
    ## DEF

    def inShardKeyPattern(self, col_name, attr) :
        return attr in self.data[col_name]['shardKeys']
    ## DEF

    ## ----------------------------------------------
    ## INDEXES
    ## ----------------------------------------------

    def getIndexes(self, col_name) :
        return self.data[col_name]['indexes']
    ## DEF

    def getAllIndexes(self) :
        return dict(self.data.iteritems())
    ## DEF

    def addIndex(self, col_name, indexKeys):
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
    
    def hasIndex(self, col_name, list) :
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
        for col_name in sorted(self.data.iterkeys()):
            ret += "[%02d] %s\n" % (ctr, col_name)
            for k, v in self.data[col_name].iteritems():
                ret += "  %-10s %s\n" % (k+":", v)
            ctr += 1
        return ret
    ## DEF

    def toJSON(self) :
        return json.dumps(self.toDICT(), sort_keys=False, indent=4)

    def toDICT(self) :
        return self.data
    ## DEF

## CLASS