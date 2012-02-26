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
    'query_use_count' : 0,
    'hist_query_keys' : [],
    'hist_query_values' : [],
    'hist_data_keys' : [],
    'hist_data_values' : [],
    'max' : Maximum Value,
    'min' : Minimum Value,
}
'''