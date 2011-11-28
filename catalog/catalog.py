# -*- coding: utf-8 -*-

import sys
import logging
from pprint import pformat

import collection
import key

LOG = logging.getLogger(__name__)

def generateCatalogFromDatabase(db):
    """Generate a catalog for the given database"""
    
    catalog = Catalog()
    for coll_name in db.collection_names():
        if coll_name.startswith("system."): continue
        if not coll_name.startswith("CUSTOMER"): continue # FIXME
        
        # COLLECTION SCHEMA
        # HACK: Grab one document from the collection and pick it apart to see what keys it has
        doc = db[coll_name].find_one()
        if not doc:
            LOG.warn("Failed to retrieve at least one record from %s.%s" % (db.name, coll_name))
            continue
        coll_data = parseDocument(doc)
        
        # COLLECTION INDEXES
        indexes = db[coll_name].index_information()
        
        # SHARDING KEYS
        shard_keys = [ ] # TODO
        
        coll_catalog = collection.Collection(coll_name, coll_data, shard_keys, indexes)
        catalog.collections.append(coll_catalog)
    ## FOR
    
    return (catalog)
## DEF

def parseDocument(doc, data={}):
    """Parse a single document and extract out the keys"""
    for name,val in doc.items():
        val_type = type(val)
        data[name] = key.Key(name, val_type)
        
        if val_type == list:
            data[name].inner = [ ]
            for list_val in val:
                data[name].inner.append({ })
                parseDocument(list_val, data[name].inner[-1])
            ## FOR
        elif val_type == dict:
            data[name].inner = { }
            parseDocument(val, data[name].inner)
    ## FOR
    return (data)
## DEF

## ==============================================
## Catalog
## ==============================================
class Catalog(object):
    def __init__(self, collections=[ ]):
        self.collections = collections
    
    def __str__(self):
        return self.__unicode__()
    def __repr__(self):
        ret = [ ]
        for c in self.collections:
            ret.append(str(c))
        return pformat(ret)