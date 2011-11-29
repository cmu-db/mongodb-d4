# -*- coding: utf-8 -*-

import sys
import logging
import types
from pprint import pformat
from mongokit import Document, CustomType

from util import *

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
    required_fields = ['name', 'fields', 'shard_keys', 'indexes']
    
    def getEmbeddedKeys(self):
        """Return all the keys that contain embedded documents"""
        ret = [ ]
        for catalog_key in self["fields"].values():
            if catalog_key.type in [list, dict]:
                ret.append(catalog_key)
        ## FOR
        return (ret)
    ## DEF

    #def __str__(self):
        #return self.__unicode__()
    #def __unicode__(self):
        #return pformat(self.__dict__)
    #def __repr__(self):
        #return pformat(self.__dict__)

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
            for t in [ str, bool ]:
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
class Field(Document):
    __collection__ = constants.CATALOG_FIELDS
    structure = {
        'name': unicode,
        'type': FieldType,
        'min_size': int,
        'max_size': int
    }
    required_fields = ['name', 'type']
        
    def __str__(self):
        return self.__unicode__()
    def __unicode__(self):
        return "%s[%s]" % (self.name, self.type.__name__)