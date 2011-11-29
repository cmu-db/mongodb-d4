# -*- coding: utf-8 -*-

import sys
import logging
from pprint import pformat
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
        'shard_keys': [unicode],
        'indexes': dict,
    }
    required_fields = ['name', 'fields', 'shard_keys', 'indexes']
    
    def __init__(self, name, fields={}, shard_keys=[], indexes={}):
        self.name = name
        self.fields = fields
        self.shard_keys = shard_keys[:]
        self.indexes = indexes
    ## DEF

    def getEmbeddedKeys(self):
        """Return all the keys that contain embedded documents"""
        ret = [ ]
        for catalog_key in self.fields.values():
            if catalog_key.type in [list, dict]:
                ret.append(catalog_key)
        ## FOR
        return (ret)
    ## DEF

    def __str__(self):
        return self.__unicode__()
    def __unicode__(self):
        return pformat(self.__dict__)
    def __repr__(self):
        return pformat(self.__dict__)
        