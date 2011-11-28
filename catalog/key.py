# -*- coding: utf-8 -*-

import sys
import logging
from pprint import pformat

## ==============================================
## Key
## ==============================================
class Key(object):

    def __init__(self, name, type, inner=None):
        self.name = name
        self.type = type
        self.inner = inner
        
        ## TODO: Extract statistics about this key
        self.min_size = None
        self.max_size = None
    ## DEF
        
    def __str__(self):
        return self.__unicode__()
    def __unicode__(self):
        return "%s[%s]" % (self.name, self.type.__name__)
    def __repr__(self):
        ret = { }
        for k,v in self.__dict__.items():
            if v != None: ret[k] = v
        return pformat(ret)