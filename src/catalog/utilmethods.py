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
        LOG.info("Retrieving schema information from %s.%s" % (dataset_db.name, coll_name))
        
        # COLLECTION SCHEMA
        # Grab one document from the collection and pick it apart to see what keys it has
        # TODO: We should probably sample more than one document per collection
        doc = dataset_db[coll_name].find_one()
        if not doc:
            LOG.warn("Failed to retrieve at least one record from %s.%s" % (dataset_db.name, coll_name))
            continue
        
        coll_catalog = schema_db.Collection()
        coll_catalog['name'] = coll_name
        try:
            coll_catalog["fields"] = extractFields(doc)
        except:
            LOG.error("Unexpected error when processing %s.%s" % (dataset_db.name, coll_name))
            raise
        
        # COLLECTION INDEXES
        coll_catalog["indexes"] = dataset_db[coll_name].index_information()
        
        # SHARDING KEYS
        coll_catalog["shard_keys"] = [ ] # TODO

        coll_catalog.save()
        #print coll_catalog
    ## FOR
    #print "-"*100
    #print schema_db.catalog.find_one({'name': 'CUSTOMER'})
## DEF

def extractFields( doc, fields={ }):
    """Parse a single document and extract out the keys"""
    for name,val in doc.items():
        # TODO: Should we always skip '_id'?
        if name == '_id': continue

        fields[name] = fields.get(name, { })
        
        # TODO: What do we do if we get back an existing field 
        # that has a different type? Does it matter? Probably not...
        val_type = type(val)
        fields[name]['type'] = fieldTypeToString(val_type)
        
        # TODO: Build a histogram and keep track of the min/max sizes
        # for the values of this field
        fields[name]['min_size'] = None
        fields[name]['max_size'] = None
        
        if val_type == list:
            # TODO: Build a histogram based on how long the list is
            fields[name]['fields'] = { }
            for list_val in val:
                if isinstance(list_val, dict):
                    extractFields(list_val, fields[name]['fields'])
                else:
                    # TODO: Add support for single values embedded in lists
                    assert(False)
            ## FOR
        elif val_type == dict:
            fields[name]['fields'] = { }
            extractFields(val, fields[name]['fields'])
    ## FOR
    return (fields)
## DEF

def fieldTypeToString(pythonType):
    return unicode(pythonType.__name__)

def fieldTypeToPython(strType):
    for t in [ str, bool, datetime ]:
        if strType == t.__name__: return t
    return eval("types.%sType" % strType.title())