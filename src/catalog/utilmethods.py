# -*- coding: utf-8 -*-

import sys
import logging
import re
import types
import math
from datetime import datetime
from pprint import pformat

from collection import Collection
from util import constants

#logging.basicConfig(level = logging.DEBUG,
                    #format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    #datefmt="%m-%d-%Y %H:%M:%S",
                    #stream = sys.stdout)
LOG = logging.getLogger(__name__)

# Mapping from TypeName -> Type
TYPES_XREF = dict([ (t.__name__, t) for t in [types.IntType, types.LongType, types.FloatType, types.BooleanType] ])

def getEstimatedSize(typeName, value):
    """Returns the estimated size (in bytes) of the value for the given type"""
    assert type(typeName) in [unicode, str], "Invalid typeName '%s/%s'" % (typeName, type(typeName))
    
    # DATETIME
    if typeName == 'datetime':
        return (8) # XXX
    # STR
    elif typeName in [types.StringType.__name__, types.UnicodeType.__name__]:
        return getStringSize(value)
    # OBJECT ID
    elif typeName == "ObjectId":
        return getStringSize(str(value))
    # NONE
    elif not typeName or typeName == types.NoneType.__name__:
        return (0)

    # Everything else
    assert typeName in TYPES_XREF, "Unexpected type '%s'" % typeName
    realType = TYPES_XREF[typeName]
    return realType.__sizeof__(value)
## DEF

# Regex for extracting anonymized strings
ANONYMIZED_STR_REGEX = re.compile("([\w]{32})\/([\d]+)")
def getStringSize(s):
    """Returns the length of the string. We will check whether the string
       is one our special anoymized strings"""
    match = ANONYMIZED_STR_REGEX.match(s)
    if match:
        return int(match.group(2))
    else:
        return len(s)
## DEF

def sqlTypeToPython(sqlType):
    sqlType = sqlType.lower()
    if sqlType.endswith('int') :
        t = types.IntType
    elif sqlType.endswith('double') :
        t = float
    elif sqlType.endswith('text') or sqlType.endswith('char'):
        t = types.StringType
    elif sqlType.endswith('time'):
        t = datetime
    elif sqlType.endswith('stamp'):
        t = datetime
    elif sqlType.endswith('binary') :
        t = str
    elif sqlType.endswith('blob') :
        t = str
    else:
        raise Exception("Unexpected SQL type '%s'" % sqlType)
    return (t)
## DEF

def fieldTypeToString(pythonType):
    return unicode(pythonType.__name__)

def fieldTypeToPython(strType):
    for t in [ str, bool, datetime ]:
        if strType == t.__name__: return t
    return eval("types.%sType" % strType.title())
    
def variance_factor(list, norm):
    n, mean, std = len(list), 0, 0
    if n <= 1 or norm == 0 :
        return 0
    else :
        for a in list:
            mean = mean + a
        mean /= float(n)
        for a in list:
            std += (a - mean)**2
        std = math.sqrt(std / float(n-1))
        return abs(1 - (std / norm))
## DEF

def getAllValues(fields):
    """
        Return a tuple of all the values in the fields dict
        The fields will be sorted lexiographically so that two documents with
        the same values always come back with the same tuple
    """

    def _getListValues(value_list):
        values = [ ]
        for v in value_list:
            if isinstance(v, dict):
                values.append(getAllValues(v))
            elif isinstance(v, list):
                values.append(_getListValues(v))
            else:
                values.append(v)
            ## FOR
        return tuple(values)
    ## DEF

    values = [ ]
    for k in sorted(fields.keys()):
        v = fields[k]
        if isinstance(v, dict):
            values.append(getAllValues(v))
        elif isinstance(v, list):
            values.append(_getListValues(v))
        else:
            values.append(v)
        ## FOR
    return tuple(values)
## DEF

def getFieldValues(fieldNames, fields):
    """
        Return a tuple of the values for the given list of shardingKeys
    """
    values = [ ]
    for fieldName in fieldNames:
        values.append(getFieldValue(fieldName, fields))
    return tuple(values)
## DEF


def getFieldValue(fieldName, fields):
    """
        Return the field value for the given shardingKey entry
        The shardKey can be a nested field using dot notation
    """

    # If the sharding key has a dot in it, then we will want
    # to fix the prefix and then traverse further into the fields
    splits = fieldName.split(".")
    if not splits[0] in fields:
        return None
    elif len(splits) > 1:
        return getFieldValue(fieldName[len(splits[0])+1:], fields[splits[0]])

    # Check whether the value is a dict that has only one key with our special
    # marking character. If it does, then that's the real value that we want to return
    # This will happen when there are things like range predicates (Example {"#gt": 123})
    # Or if it is a special field type for MongoDB (Example {"#date": 123456})
    value = fields[fieldName]
    if isinstance(value, dict):
        if len(value.keys()) == 1:
            key = value.keys()[0]
            if key.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX):
                # TODO: Need to handle nested values better
                value = value[key]
                if isinstance(value, list):
                    value = tuple(value)
        elif len(value.keys()) == 2: # This will happen when there are things like range predicates (Example {"#gt": 123})
            keys = value.keys()
            if keys[0].startswith(constants.REPLACE_KEY_DOLLAR_PREFIX) and keys[1].startswith(constants.REPLACE_KEY_DOLLAR_PREFIX):
                values = value.values()
                value = tuple(values)
    ## IF

    return value
## DEF

def hasField(fieldName, fields):
    """
        Return the field value for the given shardingKey entry
        The shardKey can be a nested field using dot notation
    """
    # If the sharding key has a dot in it, then we will want
    # to fix the prefix and then traverse further into the fields
    splits = fieldName.split(".")
    keys_in_fields = []
    for field in fields:
        keys_in_fields.extend(field.keys())
    if not splits[0] in keys_in_fields:
        return False
    elif len(splits) > 1:
        return getFieldValue(fieldName[len(splits[0])+1:], fields[splits[0]])

    return True
## DEF
