# -*- coding: utf-8 -*-

import sys
import logging
from pprint import pformat

import collection

LOG = logging.getLogger(__name__)

def generateCatalogFromDatabase(dataset_db, schema_db):
    """Generate a catalog for the given database"""
    
    for coll_name in dataset_db.collection_names():
        if coll_name.startswith("system."): continue
        if not coll_name.startswith("CUSTOMER"): continue # FIXME
        
        # COLLECTION SCHEMA
        # HACK: Grab one document from the collection and pick it apart to see what keys it has
        doc = dataset_db[coll_name].find_one()
        if not doc:
            LOG.warn("Failed to retrieve at least one record from %s.%s" % (dataset_db.name, coll_name))
            continue
        
        coll_catalog = schema_db.Collection()
        coll_catalog["fields"] = parseDocument(schema_db, doc)
        
        # COLLECTION INDEXES
        coll_catalog["indexes"] = dataset_db[coll_name].index_information()
        
        # SHARDING KEYS
        coll_catalog["shard_keys"] = [ ] # TODO

        coll_catalog.save()
    ## FOR
## DEF

def parseDocument(schema_db, doc, data={}):
    """Parse a single document and extract out the keys"""
    for name,val in doc.items():
        val_type = type(val)
        f = schema_db.Field()
        f['name'] = name
        f['type'] = val_type
        
        if val_type == list:
            data[name].inner = [ ]
            for list_val in val:
                data[name].inner.append({ })
                parseDocument(list_val, data[name].inner[-1])
            ## FOR
        elif val_type == dict:
            data[name].inner = { }
            parseDocument(val, f.inner)
        data[name] = f
    ## FOR
    return (data)
## DEF