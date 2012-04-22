#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
import os
import sys
import argparse
import logging
import pymongo
import mongokit
from pprint import pprint
from ConfigParser import SafeConfigParser

import catalog
import workload
import search
import random
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s\n%s" % (constants.PROJECT_NAME, constants.PROJECT_URL))
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--host', type=str, default="localhost",
                         help='The hostname of the MongoDB instance containing the sample workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file used by %s' % constants.PROJECT_NAME)
    aparser.add_argument('--reset', action='store_true', help='Reset collection statistics')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    if args['print_config']:
        print config.makeDefaultConfig()
        sys.exit(0)
    
    if not args['config']:
        logging.error("Missing configuration file")
        print
        aparser.print_help()
        sys.exit(1)
    logging.debug("Loading configuration file '%s'" % args['config'])
    cparser = SafeConfigParser()
    cparser.read(os.path.realpath(args['config'].name))
    config.setDefaultValues(cparser)
    
    ## ----------------------------------------------
    ## Connect to MongoDB
    ## ----------------------------------------------
    hostname = cparser.get(config.SECT_MONGODB, 'hostname')
    port = cparser.getint(config.SECT_MONGODB, 'port')
    assert hostname
    assert port
    try:
        conn = mongokit.Connection(host=hostname, port=port)
    except:
        LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
        raise
    ## Register our objects with MongoKit
    conn.register([ catalog.Collection, workload.Session ])

    ## FOR
    metadata_db = conn[cparser.get(config.SECT_MONGODB, 'metadata_db')]
    dataset_db = conn[cparser.get(config.SECT_MONGODB, 'dataset_db')]
    
    ## -----------------------------------------------------
    ## Step 1: Preprocessing & Zero statistics if required
    ## -----------------------------------------------------
    sample_rate = cparser.getint(config.SECT_DESIGNER, 'sample_rate')
    first = {}
    collections = metadata_db.Collection.find()
    distinct_values = {}
    for col in collections :
        distinct_values[col['name']] = {}
        first[col['name']] = {}
        for k, v in col['fields'].iteritems() :
            distinct_values[col['name']][k] = {}
            first[col['name']][k] = True
    
    if args['reset'] :
        collections = metadata_db.Collection.find()
        for col in collections :
            for k, v in col['fields'].iteritems() :
                v['query_use_count'] = 0
                v['cardinality'] = 0
                v['selectivity'] = 0
            col.save()
    
    ## ----------------------------------------------
    ## Step 2: Process Workload Trace
    ## ----------------------------------------------
    for rec in metadata_db[constants.COLLECTION_WORKLOAD].find() :
        for op in rec['operations'] :
            tuples = []
            col_info = metadata_db.Collection.one({'name':op['collection']})
            if op['type'] == '$delete' :
                for content in op['content'] :
                    for k,v in content.iteritems() :
                        tuples.append((k, v))
            elif op['type'] == '$insert' :
                for content in op['content'] :
                    for k,v in content.iteritems() :
                        tuples.append((k, v))
            elif op['type'] == '$query' :
                for content in op['content'] :
                    if content['query'] != None :
                        for k, v in content['query'].iteritems() :
                            tuples.append((k, v))
            elif op['type'] == '$update' :
                for content in op['content'] :
                    for k,v in content.iteritems() :
                        tuples.append((k, v))
            for t in tuples :
                ## Update times the column is referenced in a query
                col_info['fields'][t[0]]['query_use_count'] += 1
            col_info.save()
    
    ## ----------------------------------------------
    ## Step 3: Process Dataset
    ## ----------------------------------------------
    collections = metadata_db.Collection.find()
    tuple_sizes = {}
    for col in collections :
        col['tuple_count'] = 0
        tuple_sizes[col['name']] = 0
        rows = dataset_db[col['name']].find()
        for row in rows :
            col['tuple_count'] += 1
            to_use = random.randrange(1, 100, 1)
            if to_use <= sample_rate : 
                for k, v in row.iteritems() :
                    if k <> '_id' :
                        if col['fields'][k]['type'] == 'int' :
                            tuple_sizes[col['name']] += 4
                        elif col['fields'][k]['type'] == 'str' :
                            tuple_sizes[col['name']] += len(v)
                        elif col['fields'][k]['type'] == 'datetime' :
                            tuple_sizes[col['name']] += 8
                        elif col['fields'][k]['type'] == 'float' :
                            tuple_sizes[col['name']] += 8
                        distinct_values[col['name']][k][v] = v
                    else :
                        tuple_sizes[col['name']] += 12
        col['avg_doc_size'] = int(tuple_sizes[col['name']] / col['tuple_count'])
        col.save()
    
    ## ---------------------------------------------
    ## Step 4: Calculate cardinality and selectivity
    ## ---------------------------------------------
    collections = metadata_db.Collection.find()
    for col in collections :
        for k,v in col['fields'].iteritems() :
            v['cardinality'] = len(distinct_values[col['name']][k])
            if col['tuple_count'] == 0 :
                v['selectivity'] = 0
            else :
                v['selectivity'] = v['cardinality'] / col['tuple_count']
        col.save()
## END MAIN