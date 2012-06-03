# -*- coding: utf-8 -*-

import logging

from util import constants

LOG = logging.getLogger(__name__)

def escapeFieldNames(content):
    """Fix key names so that they can be stored in MongoDB"""
    copy = dict(content.items())
    toFix = [ ]
    for k, v in copy.iteritems():
        # Keys can't start with '$' and they can't contain '.'
        if k.startswith('$') or k.find(".") != -1:
            toFix.append(k)
        if type(v) == dict:
            v = escapeFieldNames(v)
        elif type(v) == list:
            for i in xrange(0, len(v)):
                if type(v[i]) == dict:
                    v[i] = escapeFieldNames(v[i])
            ## FOR
        copy[k] = v
    ## FOR
    
    for k in toFix:
        v = copy[k]
        del copy[k]
        
        if k.startswith('$'):
            k = constants.REPLACE_KEY_DOLLAR_PREFIX + k[1:]
        k = k.replace(".", constants.REPLACE_KEY_PERIOD)
        copy[k] = v
    ## FOR
    
    return copy
## DEF
