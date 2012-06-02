# -*- coding: utf-8 -*-

import sys
import logging
import types
from datetime import datetime
from pprint import pformat

import collection
import math

LOG = logging.getLogger(__name__)

def extractFields(self, fields, doc, nested=False):
    """Recursively traverse a single document and extract out the field information"""
    for k,v in doc.iteritems():
        
        # TODO: Should we always skip '_id'?
        # if name == '_id': continue
        
        f_type = type(v)
        f_type_str = fieldTypeToString(f_type)
        
        if not k in fields:
            # This is only subset of what we will compute for each field
            # See catalog.Collection for more information
            fields[k] = {
                'type': f_type_str,
                'fields': { },
            }
        else:
            # Sanity check
            # This won't work if the data is not uniform
            assert fields[k]['type'] == f_type_str, \
                "Mismatched field types '%s' <> '%s'" % (fields[k]['type'], f_type_str)
        
        # Nested Fields
        if f_type is dict:
            self.extractFields(fields[k]['fields'], doc[k], True)
        
        # List of Values
        # Could be either scalars or dicts. If it's a dict, then we'll just
        # store the nested field information in the 'fields' value
        # If it's a list, then we'll use a special marker 'LIST_INNER_FIELD' to
        # store the field information for the inner values.
        elif f_type is list:
            for i in xrange(0, len(doc[k])):
                inner_type = type(doc[k][i])
                # More nested documents...
                if inner_type is dict:
                    self.extractFields(fields[k], doc[k][i], True)
                else:
                    # TODO: We probably should store a list of types here in case
                    #       the list has different types of values
                    inner = fields[k]['fields'].get(constants.LIST_INNER_FIELD], {})
                    inner['type'] = fieldTypeToString(inner_type)
            ## FOR (list)
    ## FOR
## DEF

# Mapping from TypeName -> Type
TYPES_XREF = { (t.__name__, t) for t in [IntType, LongType, FloatType, BooleanType] }
def getEstimatedSize(typeName, value):
    """Returns the estimated size (in bytes) of the value for the given type"""
    
    # DATETIME
    if typeName == 'datetime':
        return (8) # XXX
    # STR
    elif typeName == StringType.__name__:
        return getStringSize(value)
    
    # Everything else
    realType = TYPES_XREF[typeName]
    assert realType, "Unexpected type '%s'" % typeName
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
    
def gatherStatisticsFromCollections(collectionsIterable) :
    '''
    Gather statistics from an iterable of collections for using in instantiation of
    the cost model and for determining the initial design solution
    '''
    statistics = {}
    statistics['total_queries'] = 0
    for col in collectionsIterable :
        statistics[col['name']] = {
            'fields' : {},
            'tuple_count' : col['tuple_count'],
            'workload_queries' : 0,
            'workload_percent' : 0.0,
            'avg_doc_size' : col['avg_doc_size'],
            'interesting' : [],
        }
        for field, data in col['fields'].iteritems() :
            if data['query_use_count'] > 0 :
               statistics[col['name']]['interesting'].append(field)
            statistics[col['name']]['fields'][field] = {
                'query_use_count' : data['query_use_count'],
                'cardinality' : data['cardinality'],
                'selectivity' : data['selectivity']
            }
    return statistics
    
def variance_factor(list, norm):
    n, mean, std = len(list), 0, 0
    if n <= 1 or norm == 0 :
        return 0
    else :
        for a in list:
            mean = mean + a
        mean = mean / float(n)
        for a in list:
            std = std + (a - mean)**2
        std = math.sqrt(std / float(n-1))
        return abs(1 - (std / norm))