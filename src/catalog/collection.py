# -*- coding: utf-8 -*-

from mongokit import Document

from util import *

## ==============================================
## Collection
## ==============================================
class Collection(Document):
    __collection__ = constants.COLLECTION_SCHEMA
    structure = {
        'name':             basestring,# The name of the collection
        'shard_keys':       dict,      # The original sharding keys assigned for this collection (if available)
        'indexes':          [dict],    # The original index keys assigned for this collection (if available)
        'data_size':        long,      # The estimated total size of this collection
        'doc_count':        int,       # The estimated number of documents in this collection
        'avg_doc_size':     int,       # The average size of the documents in the collection (bytes)
        'max_pages':        int,       # The maximum number of pages required to scan the collection
        'workload_queries': int,       # The number operations that reference this collection
        'workload_percent': float,     # The percentage of the total workload that touch this collection
        'interesting':      [basestring], # TODO(ckeith)
        'embedding_ratio':  dict,      # The ratio between documents for foreign key values in parent collection and that in child collection

        ## ----------------------------------------------
        ## FIELDS
        ## ----------------------------------------------
        'fields': {
            basestring: {
                'type':              basestring, # catalog.fieldTypeToString(col_type),
                'fields':            dict,       # nested fields
                'query_use_count':   int,        # The number of times this field is referenced in queries
                'cardinality':       int,        # Number of distinct values
                'selectivity':       float,      # Cardinality / Number of all values
                'avg_size':          int,        # The average size of the values for this field (bytes)
                'parent_col':        basestring, # TODO(ckeith)
                'parent_key':        basestring, # TODO(ckeith)
                'parent_candidates': list,       # List of (parent_col, parent_key) candidates 
                
                # SQL Parameters
                'ordinal_position':   int,       # The original position of this field in the table
                
                # List Field Attributes
                'list_len_min':     int,
                'list_len_max':     int,
                'list_len_avg':     float,
                'list_len_stdev':   float,
            }
        }
    }
    
    required_fields = [
        'name', 'doc_count'
    ]
    default_values = {
        'shard_keys':           { },
        'indexes':              [ ],
        'doc_count':            0,
        'avg_doc_size':         0,
        'max_pages':            0,
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
            'selectivity':      0.0,
            'avg_size':         0,
            'parent_col':       None,
            'parent_key':       None,
            'parent_candidates': [ ],
            'ordinal_position': None,
            'list_len_min':     None,
            'list_len_max':     None,
            'list_len_avg':     None,
            'list_len_stdev':   None,
        }
        return (field)
    ## DEF

    def getField(self, f_name, fields=None):
        if not fields: fields = self['fields']

        # If the field name has a dot in it, then we will want
        # to fix the prefix and then traverse further into the fields
        splits = f_name.split(".")
        if not splits[0] in fields:
            return None
        elif len(splits) > 1:
            return self.getField(f_name[len(splits[0])+1:], fields[splits[0]]['fields'])
        return fields[f_name]
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