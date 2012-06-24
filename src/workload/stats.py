# -*- coding: utf-8 -*-

from mongokit import Document
import sys
sys.path.append("../")
from util import *

## ==============================================
## Workload Statistics
## ==============================================
class Stats(Document):
    __collection__ = constants.COLLECTION_STATS
    structure = {
        'name':              unicode,   # The name of the collection
        'tuple_count':       int,       # The number of tuples in the collection
        'workload_queries':  int,       # The number of queries in the workload
        'workload_percent':  float,     # TODO(ckeith)
        'avg_doc_size':      float,     # The average size of the documents in the collection (byte)
        'interesting':       [unicode], # TODO(ckeith)
        
        ## ----------------------------------------------
        ## FIELDS
        ## ----------------------------------------------
        'fields': {
            unicode: {
                # The number of times this field is referenced in queries
                'query_use_count':      int,
                
                # Value Cardinality
                'cardinality':          float,
                
                # Value Selectivity
                'selectivity':          float,
            }
        ],
    }
    required_fields = [
        'name', 'tuple_count',
    ]
    default_values = {
        'workload_queries':     0,
        'workload_percent':     0.0,
        'interesting':          [ ],
        'fields':               { },
    }

## CLASS