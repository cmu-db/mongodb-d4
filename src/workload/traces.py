# -*- coding: utf-8 -*-

from mongokit import Document
import sys
sys.path.append("../")
from util import *

## ==============================================
## Session
## ==============================================
class Session(Document):
    __collection__ = constants.WORKLOAD_SESSIONS
    structure = {
        'ip_client':  unicode,  # IP:port of the client
        'ip_server':  unicode,  # IP:port of the mongo server
        'session_id':  int,     # our incremental session number added by the parser
        'operations': [
            {
                'collection':   unicode,    # the name of the collection
                'type':         unicode,    # the type of the query ($delete, $insert, $update, $query)
                
                'query_time':   float,      # timestamp of the query (from sniff)
                'resp_time':    float,      # timestamp of the response
                'query_content':list,       # payload of the query (BSON)
                'resp_content': dict,       # payload of the response (list of BSON objs)
                'query_size':   int,        # query payload [bytes]
                'resp_size':    int,        # response payload [bytes]
                # query_id and resp_id are used to pair up queries & responses
                'query_id':     int,        # unique ID from mongosniff
                'resp_id':      int,        # ID from mongosniff
                'query_hash':   int,        # from ophasher
                
                # sql2mongo
                'query_group':  int,        # sql2mongo split join
                
                # query flags & props
                # flags: 1==upsert:TRUE, multi:FALSE, 2==upsert:FALSE, multi:TRUE
                'update_upsert': int,       # T/F from flags
                'update_multi':  int,       # T/F from flags
                'query_limit':  int,        # ntoreturn, -1: all
                'query_offset': int,        # ntoskip
                'query_aggregate': int,     # T/F aggregate yes or no
                #'has_related_query': int   # I don't think we've talked about this one - ebuzek
            }
        ],
    }
    required_fields = [
        'ip_client', 'ip_server',
        #'operations.collection', 'operations.timestamp', 'operations.content',
        #'operations.type', 'opreations.size',
    ]

## CLASS