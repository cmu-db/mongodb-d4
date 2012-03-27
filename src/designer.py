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

    ## Make sure that the databases that we need are there
    db_names = conn.database_names()
    for key in [ 'dataset_db', ]: # FIXME 'workload_db' ]:
        if not cparser.has_option(config.SECT_MONGODB, key):
            raise Exception("Missing the configuration option '%s.%s'" % (config.SECT_MONGODB, key))
        elif not cparser.get(config.SECT_MONGODB, key):
            raise Exception("Empty configuration option '%s.%s'" % (config.SECT_MONGODB, key))
        db_name = cparser.get(config.SECT_MONGODB, key)
        if not db_name in db_names:
            raise Exception("The %s database '%s' does not exist" % (key.upper(), db_name))
    ## FOR
    metadata_db = conn[cparser.get(config.SECT_MONGODB, 'metadata_db')]
    dataset_db = conn[cparser.get(config.SECT_MONGODB, 'dataset_db')]

    ## ----------------------------------------------
    ## STEP 1
    ## Generate an initial solution
    ## ----------------------------------------------
    
    params = {
        'num_queries' : 0.2,
        'num_query_keys' : 0.2,
        'dist_query_keys' : 0.2,
        'num_data_keys' : 0.2,
        'dist_data_keys' : 0.2
    }
    collections = metadata_db.Collection.find()
    statistics = {}
    
    for col in collections :
        statistics[col['name']] = {}
        total_queries = 0
        for field, data in col['fields'].iteritems() :
            statistics[col['name']][field] = {}
            total_queries += data['query_use_count']
        for field, data in col['fields'].iteritems() :
            if total_queries == 0 :
                statistics[col['name']][field]['num_queries'] = 0
            else :
                print field, data['query_use_count'] , total_queries
                statistics[col['name']][field]['num_queries'] = data['query_use_count'] / total_queries
            statistics[col['name']][field]['num_query_keys'] = len(data['hist_query_keys'])
            statistics[col['name']][field]['num_data_keys'] = len(data['hist_data_keys'])
        
        print statistics[col['name']]
        
    ## ----------------------------------------------
    ## STEP 2
    ## Execute the LNS design algorithm
    ## ----------------------------------------------
    
## MAIN