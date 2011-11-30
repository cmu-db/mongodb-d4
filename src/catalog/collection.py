# -*- coding: utf-8 -*-

import sys
import logging
import types
from datetime import datetime
from pprint import pformat
from mongokit import Document, CustomType
from bson import BSON

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
            

    def validate(self, value, path):
        """OPTIONAL : useful to add a validation layer"""
        if value is not None:
            pass #  do something here
## CLASS

# ==============================================
# Field
# ==============================================
class Field(CustomType):
    mongo_type = unicode
    python_type = dict
    init_type = None
    structure = {
        'name': unicode,
        'type': FieldType(),
        'min_size': int,
        'max_size': int,
        #'inner': Field,
    }
    
    def to_bson(self, value):
        """convert type to a mongodb type"""
        new_value = dict(value.items())
        new_value['type'] = Field.structure['type'].to_bson(value['type'])
        print "to_bson ->", new_value
        return BSON.from_dict(new_value)

    def to_python(self, value):
        """convert type to a python object"""
        if value is not None:
            new_value = value.to_dict()
            new_value['type'] = Field.structure['type'].to_python(new_value['type'])
            print "to_python ->", type(value), "==>", new_value
            return new_value

    def validate(self, value, path):
        """OPTIONAL : useful to add a validation layer"""
        if value is not None:
            pass #  do something here
## CLASS
        
## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.CATALOG_COLL
    structure = {
        'name': unicode,
        'fields': dict,
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
