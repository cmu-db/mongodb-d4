# -*- coding: utf-8 -*-

from mongokit import Document

from util import *

## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.CATALOG_COLL
    structure = {
        'name': unicode,
        'fields': dict,
        'shard_key': [ ],
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