# -*- coding: utf-8 -*-

import logging
from util import constants
from pprint import pformat

LOG = logging.getLogger(__name__)

def isOpRegex(op, field=None):
    """Returns true if this operation contains a regex query"""

#    if "predicates" in op:
#        return constants.PRED_TYPE_REGEX in op["predicates"].itervalues()

    regex_flag = constants.REPLACE_KEY_DOLLAR_PREFIX + "regex"
    for contents in getOpContents(op):
        if field is None:
            for k, v in contents.iteritems():
                if isinstance(v, dict) and regex_flag in v:
                    return True
        elif field in contents:
            if isinstance(contents[field], dict) and regex_flag in contents[field]:
                return True
    ## FOR
    return False
## FOR

def getOpContents(op):
    """Return a list of all of the query contents for the given operation"""
    # QUERY
    if op['type'] == constants.OP_TYPE_QUERY:
        # TODO: Why are we not examining the resp_content here?
        contents = [ ]
        for opContent in op['query_content']:
            try:
                if '#query' in opContent and opContent['#query']:
                    contents.append(opContent['#query'])
            except:
                LOG.error("Invalid query content:\n%s", pformat(opContent))
                raise

    # INSERT + UPDATE + DELETE
    elif op['type'] in [constants.OP_TYPE_INSERT, constants.OP_TYPE_UPDATE, constants.OP_TYPE_DELETE]:
        contents = op['query_content']

    return contents
## DEF

## ==============================================
## OLD STUFF
## ==============================================

@DeprecationWarning
def getReferencedFields(op):
    """Get a list of the fields referenced in the given operation"""
    content = op["query_content"]
    
    # QUERY
    if op["type"] == constants.OP_TYPE_QUERY:
        if not op["query_aggregate"]: 
            fields = content[parser.OP_TYPE_QUERY].keys()
    # DELETE
    elif op["type"] == constants.OP_TYPE_DELETE:
        fields = content.keys()

    # UPDATE
    elif op["type"] == constants.OP_TYPE_UPDATE:
        fields = set()
        for data in content:
            fields |= data.keys()
        fields = list(fields)
        
    # INSERT
    elif op["type"] in [constants.OP_TYPE_INSERT, constants.OP_TYPE_ISERT]:
        fields = content.keys()
    
    return fields
## DEF


# TODO: This is just for testing that our Sessions object
# validates correctly. The parser/santizer should be fixed
# to use the Sessions object directly
@DeprecationWarning
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