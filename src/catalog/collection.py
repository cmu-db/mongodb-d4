# -*- coding: utf-8 -*-

from mongokit import Document

from util import *

## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.COLLECTION_SCHEMA
    structure = {
        'name':             unicode,   # The name of the collection
        'shard_key':        unicode,   # TODO(ckeith)
        'shard_keys':       dict,      # TODO(ckeith)
        'indexes':          [dict],    # TODO(ckeith)
        'data_size':        long,      # The estimated total size of this collection
        'doc_count':        int,       # The estimated number of documents in this collection
        'avg_doc_size':     int,       # The average size of the documents in the collection (bytes)
        'workload_queries': int,       # The number operations that reference this collection
        'workload_percent': float,     # The percentage of the total workload that touch this collection
        'interesting':      [basestring], # TODO(ckeith)

        'fields': {
            unicode: {
                'type':             basestring, # catalog.fieldTypeToString(col_type),
                'fields':           dict,       # nested fields
                'query_use_count':  int,        # The number of times this field is referenced in queries
                'cardinality':      int,        # Value Cardinality
                'selectivity':      int,        # Value Selectivity
                'parent_col':       basestring, # TODO(ckeith)
                'parent_key':       basestring, # TODO(ckeith)
                'parent_conf':      float,      # TODO(ckeith)
            }
        }

    }
    required_fields = [
        'name', 'doc_count'
    ]
    default_values = {
        'shard_key':            None,
        'shard_keys':           { },
        'indexes':              [ ],
        'doc_count':          0,
        'workload_queries':     0,
        'workload_percent':     0.0,
        'interesting':          [ ],
        'fields':               { },
    }

    @staticmethod
    def fieldFactory(fieldName, fieldType):
        """Return an uninitialized field dict that can then be inserted into this collection"""
        field = {
            'type':             fieldType,
            'fields':           { },
            'query_use_count':  0,
            'cardinality':      0,
            'selectivity':      0,
            'parent_col':       None,
            'parent_key':       None,
            'parent_conf':      None
        }
        return (field)
    ## DEF

    def getEmbeddedKeys(self):
        """Return all the keys that contain embedded documents"""
        ret = [ ]
        for catalog_key in self["fields"].values():
            if catalog_key.type in [list, dict]:
                ret.append(catalog_key)
        ## FOR
        return (ret)
    ## DEF
## CLASS