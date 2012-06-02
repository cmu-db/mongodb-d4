# -*- coding: utf-8 -*-

from mongokit import Document

from util import *

## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.COLLECTION_SCHEMA
    structure = {
        'name': unicode,
        'fields': dict,
        'shard_key': unicode,
        'shard_keys': dict,
        'indexes': dict,
        'tuple_count' : int,
        'avg_doc_size' : int,
    }
    required_fields = [ 'name' ]
    #use_autorefs = True
    
    def getEmbeddedKeys(self):
        """Return all the keys that contain embedded documents"""
        ret = [ ]
        for catalog_key in self["fields"].values():
            if catalog_key.type in [list, dict]:
                ret.append(catalog_key)
        ## FOR
        return (ret)
    ## DEF
## CLASS

'''
Dictionary for fields
{
    'type': catalog.fieldTypeToString(col_type),
    'fields': { },
    'query_use_count' : 0,
    'cardinality' : 0,
    'selectivity' : 0,
    'parent_col' : '',
    'parent_key' : '',
    'parent_conf' : 0.0,
}
'''