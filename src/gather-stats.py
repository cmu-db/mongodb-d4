#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
    
    ## ----------------------------------------------
    ## Step 1: Process Workload Trace
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
                    for k, v in content['query'].iteritems() :
                       tuples.append((k, v))
            elif op['type'] == '$update' :
                length = len(op['content'])
                i = 0
                while i < length :
                    print i
                    i += 1
            for t in tuples :
                if t[1] not in col_info['fields'][t[0]]['hist_query_keys'] :
                    col_info['fields'][t[0]]['hist_query_keys'].append(t[1])
                    col_info['fields'][t[0]]['hist_query_values'].append(1)
                else :
                    index = col_info['fields'][t[0]]['hist_query_keys'].index(t[1])
                    col_info['fields'][t[0]]['hist_query_values'][index] += 1
            col_info.save()
    
    ## ----------------------------------------------
    ## Step 2: Process Dataset
    ## ----------------------------------------------
    '''
    Stats:
    1. # of distinct values
    2. # sample histogram of how often values are used in queries
    3. # sample histogram of values that appear in dataset
    4. min/max value
    5. Histogram of how often the field in referenced in a query.
    Dictionary for fields
    {
        'type': catalog.fieldTypeToString(col_type),
        'distinct_values' : {},
        'distinct_count' : 0,
        'query_use_count' : 0,
        'hist_query_keys' : [],
        'hist_query_values' : [],
        'hist_data_keys' : [],
        'hist_data_values' : [],
        'max' : Maximum Value,
        'min' : Minimum Value,
    }
    '''
    collections = metadata_db.Collection.find()
    for col in collections :
        rows = dataset_db[col['name']].find()
        first = True
        for row in rows :
            for k, v in row.iteritems() :
                if k <> '_id' :
                    if v not in col['fields'][k]['hist_data_keys'] :
                        col['fields'][k]['hist_data_keys'].append(v)
                        col['fields'][k]['hist_data_values'].append(1)
                    else :
                        index = col['fields'][k]['hist_data_keys'].index(v)
                        col['fields'][k]['hist_data_values'][index] += 1
                    
                    if first == True :
                        col['fields'][k]['max'] = v
                        col['fields'][k]['min'] = v
                    else :
                        if v > col['fields'][k]['max'] :
                            col['fields'][k]['max'] = v
                        if v < col['fields'][k]['min'] :
                            col['fields'][k]['min'] = v
        col.save()
## END MAIN