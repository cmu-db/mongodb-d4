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
        'ip1':  unicode,
        'ip2':  unicode,
        'uid':  int,
        'operations': [
            {
                'collection':   unicode,
                'timestamp':    float,
                'content':      list,
                'output':       dict,
                'type':         unicode,
                'size':         int,
                'flags':        int,
                'query_id':     int,
            }
        ],
    }
    required_fields = [
        'ip1', 'ip2',
        #'operations.collection', 'operations.timestamp', 'operations.content',
        #'operations.type', 'opreations.size',
    ]

## CLASS