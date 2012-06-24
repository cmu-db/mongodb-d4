# -*- coding: utf-8 -*-

from mongokit import Document

from util import *

## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.COLLECTION_SCHEMA
    structure = {
        'name':         unicode,    # The name of the collection
        'shard_key':    unicode,    # TODO(ckeith)
        'shard_keys':   dict,       # TODO(ckeith)
        'indexes':      dict,       # TODO(ckeith)
        'tuple_count':  int,        # TODO(ckeith)
        'avg_doc_size': int,        # The average size of the documents in the collection (bytes)
        'interesting':  [unicode],  # TODO(ckeith)

        'fields': {
            unicode: {
                'type':             unicode,    # catalog.fieldTypeToString(col_type),
                'fields':           { },        # nested fields
                'query_use_count':  int,        # The number of times this field is referenced in queries
                'cardinality':      int,        # Value Cardinality
                'selectivity':      int,        # Value Selectivity
                'parent_col':       unicode,    # TODO(ckeith)
                'parent_key':       unicode,    # TODO(ckeith)
                'parent_conf':      float,      # TODO(ckeith)
            }
        }

    }
    required_fields = [
        'name', 'tuple_count'
    ]
    default_values = {
        'workload_queries':     0,
        'workload_percent':     0.0,
        'interesting':          [ ],
        'fields':               { },
    }
    
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