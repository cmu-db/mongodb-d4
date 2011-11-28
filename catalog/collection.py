# -*- coding: utf-8 -*-

import sys
import logging
from pprint import pformat

## ==============================================
## Collection
## ==============================================
class Collection(object):
    def __init__(self, name, data={}, shard_keys=[], indexes={}):
        self.name = name
        self.data = data
        self.shard_keys = shard_keys[:]
        self.indexes = indexes
    ## DEF

    def getEmbeddedKeys(self):
        """Return all the keys that contain embedded documents"""
        ret = [ ]
        for catalog_key in self.data.values():
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
        