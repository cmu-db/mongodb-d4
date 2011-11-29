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
        LOG.info("Retrieving schema information from %s.%s" % (dataset_db.name, coll_name))
        
        # COLLECTION SCHEMA
        # HACK: Grab one document from the collection and pick it apart to see what keys it has
        doc = dataset_db[coll_name].find_one()
        if not doc:
            LOG.warn("Failed to retrieve at least one record from %s.%s" % (dataset_db.name, coll_name))
            continue
        
        coll_catalog = schema_db.Collection()
        coll_catalog['name'] = coll_name
        coll_catalog["fields"] = extractFields(schema_db, doc)
        
        # COLLECTION INDEXES
        coll_catalog["indexes"] = dataset_db[coll_name].index_information()
        
        # SHARDING KEYS
        coll_catalog["shard_keys"] = [ ] # TODO

        print coll_catalog
        coll_catalog.save()
    ## FOR
## DEF

def extractFields(schema_db, doc, fields={ }):
    """Parse a single document and extract out the keys"""
    for name,val in doc.items():
        # TODO: Should we always skip '_id'?
        if name == '_id': continue

        val_type = type(val)
        f = schema_db.Field()
        f['name'] = name
        f['type'] = val_type
        
        #if val_type == list:
            #data[name].inner = [ ]
            #for list_val in val:
                #data[name].inner.append({ })
                #extractFields(list_val, data[name].inner[-1])
            ### FOR
        #elif val_type == dict:
            #data[name].inner = { }
            #extractFields(val, f.inner)
        f.save()
        fields[name] = f
    ## FOR
    return (fields)
## DEF