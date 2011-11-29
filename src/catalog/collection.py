# -*- coding: utf-8 -*-

import sys
import logging
import types
from pprint import pformat
from mongokit import Document, CustomType
from datetime import datetime

from util import *


## ==============================================
## FieldType
## ==============================================
class FieldType(CustomType):
    mongo_type = unicode
    python_type = types.TypeType
    init_type = None

    def to_bson(self, value):
        """convert type to a mongodb type"""
        return unicode(value.__name__)

    def to_python(self, value):
        """convert type to a python object"""
        if value is not None:
            # HACK
            for t in [ str, bool, datetime ]:
                if value == t.__name__: return t
            return eval("types.%sType" % value.title())

    def validate(self, value, path):
        """OPTIONAL : useful to add a validation layer"""
        if value is not None:
            pass #  do something here
## CLASS

## ==============================================
## Field
## ==============================================
#class Field(Document):
    #__collection__ = constants.CATALOG_FIELDS
    #structure = {
        #'name': unicode,
        #'type': FieldType(),
        #'min_size': int,
        #'max_size': int
    #}
    #required_fields = ['name', 'type']
    #default_values = {
        #'min_size': None,
        #'max_size': None
    #}
        
## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.CATALOG_COLL
    structure = {
        'name': unicode,
        'fields': {
            unicode: {
                'type': FieldType(),
                'min_size': int,
                'max_size': int
            }
        },
        'shard_keys': [ ],
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
