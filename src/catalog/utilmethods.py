# -*- coding: utf-8 -*-

import sys
import logging
import types
from datetime import datetime
from pprint import pformat

import collection
import math

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
        #print coll_catalog["name"], "=>", type(coll_catalog)
    ## FOR
    #print "-"*100
    #coll_catalog = schema_db.Collection.find_one({'name': 'CUSTOMER'})
    #print coll_catalog["name"], "=>", type(coll_catalog)
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

def sqlTypeToPython(sqlType):
    sqlType = sqlType.lower()
    if sqlType.endswith('int') :
        t = types.IntType
    elif sqlType.endswith('double') :
        t = float
    elif sqlType.endswith('text') or sqlType.endswith('char'):
        t = types.StringType
    elif sqlType.endswith('time'):
        t = datetime
    elif sqlType.endswith('stamp'):
        t = datetime
    elif sqlType.endswith('binary') :
        t = str
    elif sqlType.endswith('blob') :
        t = str
    else:
        raise Exception("Unexpected SQL type '%s'" % sqlType)
    return (t)
## DEF

def fieldTypeToString(pythonType):
    return unicode(pythonType.__name__)

def fieldTypeToPython(strType):
    for t in [ str, bool, datetime ]:
        if strType == t.__name__: return t
    return eval("types.%sType" % strType.title())
    
def gatherStatisticsFromCollections(collectionsIterable) :
    '''
    Gather statistics from an iterable of collections for using in instantiation of
    the cost model and for determining the initial design solution
    '''
    statistics = {}
    statistics['total_queries'] = 0
    for col in collectionsIterable :
        statistics[col['name']] = {
            'fields' : {},
            'tuple_count' : col['tuple_count'],
            'workload_queries' : 0,
            'workload_percent' : 0.0,
            'avg_doc_size' : col['avg_doc_size'],
            'interesting' : [],
        }
        for field, data in col['fields'].iteritems() :
            if data['query_use_count'] > 0 :
               statistics[col['name']]['interesting'].append(field)
            statistics[col['name']]['fields'][field] = {
                'query_use_count' : data['query_use_count'],
                'cardinality' : data['cardinality'],
                'selectivity' : data['selectivity']
            }
    return statistics
    
def variance_factor(list, norm):
    n, mean, std = len(list), 0, 0
    if n <= 1 or norm == 0 :
        return 0
    else :
        for a in list:
            mean = mean + a
        mean = mean / float(n)
        for a in list:
            std = std + (a - mean)**2
        std = math.sqrt(std / float(n-1))
        return abs(1 - (std / norm))