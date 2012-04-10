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
import math
    
import catalog
import workload
import search
from util import *

LOG = logging.getLogger(__name__)

def calc_stats(params, stats) :
    output = 0.0
    for k,v in params.iteritems() :
        output += v * stats[k]
    return output
    
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
        'num_queries' : 1.0,
        'num_query_keys' : 0.0,
        'dist_query_keys' : 0.0,
        'num_data_keys' : 0.0,
        'dist_data_keys' : 0.0
    }
    collections = metadata_db.Collection.find()
    statistics = {}
    results = {}
    starting_design = search.Design()
    for col in collections :
        starting_design.addCollection(col)
        statistics[col['name']] = {}
        results[col['name']] = {}
        norm_queries = 0
        norm_hqk = 0
        norm_hdk = 0
        norm_dqk = 0
        norm_ddk = 0
        col_fields = []
        for field, data in col['fields'].iteritems() :
            col_fields.append(field)
            statistics[col['name']][field] = {}
            results[col['name']][field] = 0
            if data['query_use_count'] > norm_queries :
                norm_queries = data['query_use_count']
            if len(data['hist_query_keys']) > norm_hqk :
               norm_hqk = len(data['hist_query_keys'])
            if len(data['hist_data_keys']) > norm_hdk :
               norm_hkd = len(data['hist_data_keys'])
            if len(data['hist_query_values']) == 0 :
                norm_dqk = 0
            else :
                norm_dqk = max(data['hist_query_values'])
            if len(data['hist_data_values']) == 0 :
                norm_ddk = 0
            else :
                norm_ddk = max(data['hist_data_values'])
        starting_design.addFieldsOneCollection(col, col_fields)
        for field, data in col['fields'].iteritems() :
            if norm_queries == 0 :
                statistics[col['name']][field]['num_queries'] = 0
            else :
                statistics[col['name']][field]['num_queries'] = data['query_use_count'] / norm_queries
            if norm_hqk == 0 :
                statistics[col['name']][field]['num_query_keys'] = 0
            else :
                statistics[col['name']][field]['num_query_keys'] = len(data['hist_query_keys']) / norm_hqk
            if norm_hdk == 0:
                statistics[col['name']][field]['num_data_keys'] = 0
            else : 
                statistics[col['name']][field]['num_data_keys'] = len(data['hist_data_keys']) / norm_hdk
            statistics[col['name']][field]['dist_query_keys'] = variance_factor(data['hist_query_values'], norm_dqk)
            statistics[col['name']][field]['dist_data_keys'] = variance_factor(data['hist_data_values'], norm_ddk)
        for field, data in col['fields'].iteritems() :
            results[col['name']][field] = calc_stats(params, statistics[col['name']][field])
        attr = None
        value = 0
        for field, data in results[col['name']].iteritems() :
            if data >= value :
                value = data
                attr = field
        starting_design.addShardKey(col['name'], attr)

    ## ----------------------------------------------
    ## STEP 2
    ## Execute the LNS design algorithm
    ## ----------------------------------------------
    
## MAIN