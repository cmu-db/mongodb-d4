# -*- coding: utf-8 -*-

import sys
import re
import logging
from types import *
from pprint import pformat

import traces

LOG = logging.getLogger(__name__)

# Regex for extracting anonymized strings
ANONYMIZED_STR_REGEX = re.compile("([\w]{32}\/([\d]+")

# Mapping from TypeName -> Type
TYPES_XREF = { (t.__name__, t) for t in [IntType, LongType, FloatType, BooleanType] }
TYPES_XREF['datetime'] = 

def getStringSize(s):
    """Returns the length of the string. We will check whether the string
       is one our special anoymized strings"""
    match = ANONYMIZED_STR_REGEX.match(s)
    if match:
        return int(match.group(2))
    else:
        return len(s)
## DEF

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
            k = '\\' + k
        k = k.replace(".", "__")
        copy[k] = v
    ## FOR
    
    return copy
## DEF

def getReferencedFields(op):
    """Get a list of the fields referenced in the given operation"""
    content = op["query_content"]
    
    # QUERY
    if op["type"] == parser.OP_TYPE_QUERY:
        if not op["query_aggregate"]: 
            fields = content[parser.OP_TYPE_QUERY].keys()
    # DELETE
    elif op["type"] == parser.OP_TYPE_DELETE:
        fields = content.keys()

    # UPDATE
    elif op["type"] == parser.OP_TYPE_UPDATE:
        fields = set()
        for data in content:
            fields |= data.keys()
        fields = list(fields)
        
    # INSERT
    elif op["type"] in [parser.OP_TYPE_INSERT, parser.OP_TYPE_ISERT]:
        fields = content.keys()
    
    return fields
## DEF

## ==============================================
## OLD STUFF
## ==============================================

# TODO: This is just for testing that our Sessions object
# validates correctly. The parser/santizer should be fixed
# to use the Sessions object directly
def convertWorkload(conn):
    old_workload = conn['designer']['mongo_comm']
    new_workload = ['workload']
    
    new_sess = conn['designer'].Session()
    new_sess['ip1'] = u'127.0.0.1:59829'
    new_sess['ip2'] = u'127.0.0.1:27017'
    
    for trace in old_workload.find({'IP1': new_sess['ip1'], 'IP2': new_sess['ip2']}):
        new_sess['uid'] = trace['uid']
        if not trace['content']: continue
        
        assert len(trace['content']) == 1, pformat(trace['content'])
        #print "CONTENT:", pformat(trace['content'])
        op = {
            'collection': trace['collection'],
            'content':    trace['content'][0],
            'timestamp':  float(trace['timestamp']),
            'type':       trace['type'],
            'size':       int(trace['size'].replace("bytes", "")),
        }
        new_sess['operations'].append(op)
    ## FOR
    
    print new_sess
    new_sess.save()
## DEF