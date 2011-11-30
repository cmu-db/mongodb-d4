# -*- coding: utf-8 -*-

import sys
import logging
import types
from datetime import datetime
from pprint import pformat

import collection

LOG = logging.getLogger(__name__)

def generateCatalogFromDatabase(dataset_db, schema_db):
    """Generate a catalog for the given database"""
    
    for coll_name in dataset_db.collection_names():
        if coll_name.startswith("system."): continue
        if not coll_name == 'CUSTOMER': continue
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
    print "-"*100
    print schema_db.catalog.find_one({'name': 'CUSTOMER'})
## DEF

def extractFields(schema_db, doc, fields={ }):
    """Parse a single document and extract out the keys"""
    for name,val in doc.items():
        # TODO: Should we always skip '_id'?
        if name == '_id': continue

        val_type = type(val)
        f = fields.get(name, { })
        fields[name] = f
        
        ## TODO: What do we do if we get back an existing field 
        ## that has a different type? Does it matter? Probably not...
        f['type'] = fieldTypeToString(val_type)
        
        ## TODO: Build a histogram and keep track of the min/max sizes
        ## for the values of this field
        f['min_size'] = None
        f['max_size'] = None
        
        if val_type == list:
            ## TODO: Build a histogram based on how long the list is
            f['fields'] = { }
            for list_val in val:
                extractFields(schema_db, list_val, f['fields'])
            ## FOR
        elif val_type == dict:
            f['fields'] = { }
            extractFields(schema_db, val, f['fields'])
    ## FOR
    return (fields)
## DEF

def fieldTypeToString(pythonType):
    return unicode(pythonType.__name__)

def fieldTypeToPython(strType):
    for t in [ str, bool, datetime ]:
        if value == t.__name__: return t
    return eval("types.%sType" % value.title())