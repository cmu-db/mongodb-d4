# -*- coding: utf-8 -*-

from mongokit import Document
import sys
sys.path.append("../")
from util import *

## ==============================================
## Session
## ==============================================
class Session(Document):
    __collection__ = constants.COLLECTION_WORKLOAD
    structure = {
        'ip_client':   unicode,  # IP:port of the client
        'ip_server':   unicode,  # IP:port of the mongo server
        'session_id':  int,      # our incremental session number added by the parser
        'start_time':  float,    # The relative timestamp of when this session began (in seconds)
        'end_time':    float,    # The relative timestamp of when this session finished (in seconds)
        
        ## ----------------------------------------------
        ## OPERATIONS
        ## These are all of the queries that the client executed
        ## during the session.
        ## ----------------------------------------------
        'operations': [
            {
                # The name of the collection targeted in this operation
                'collection':   unicode,
                # The type of the query ($delete, $insert, $update, $query)
                # See OPT_TYPE_* in util/constants.py
                'type':         unicode,
                
                # The relative timestamp of when the query was sent to the server (in seconds)
                'query_time':   float,
                # Query payload (BSON)
                'query_content':list,
                # Query payload size [bytes]
                'query_size':   int,
                # Unique indentifier of this query invocation
                # query_id and resp_id are used to pair up queries & responses
                'query_id':     int,
                
                # The relative timestamp of when the server returned the response (in seconds)
                'resp_time':    float,
                # Response payload (list of BSON objs)
                'resp_content': dict,
                # Response payload size [bytes]
                'resp_size':    int,        
                # Unique indentifier of the response packet
                'resp_id':      int,
                
                # A hash code compute from this query's payload signature. Different
                # invocations of queries that reference the same keys in this collection 
                # but have different input parameters will have the same hash
                # See workload/ophasher.py
                'query_hash':   int,        
                
                # sql2mongo
                'query_group':  int,        # sql2mongo split join
                
                # query flags & props
                # flags: 1==upsert:TRUE, multi:FALSE, 2==upsert:FALSE, multi:TRUE
                'update_upsert':   bool,    # T/F from flags
                'update_multi':    bool,    # T/F from flags
                'query_limit':     int,     # ntoreturn, -1: all
                'query_offset':    int,     # ntoskip
                'query_aggregate': bool,    # T/F aggregate yes or no
                
                ## ----------------------------------------------
                ## INTERNAL DATA
                ## ----------------------------------------------
                
                # A mapping from keys to their predicate types
                'predicates':  dict,     
            }
        ],
    }
    required_fields = [
        'ip_client', 'ip_server',
        #'operations.collection', 'operations.timestamp', 'operations.content',
        #'operations.type', 'opreations.size',
    ]

## CLASS