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
from search import design
import costmodel
from util import *

LOG = logging.getLogger(__name__)

def calc_stats(params, stats) :
    output = 0.0
    for k,v in params.iteritems() :
        output += v * stats[k]
    return output
    
    
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
        'query_use_count' : 1.0,
    }
    collections = metadata_db.Collection.find()
    statistics = catalog.gatherStatisticsFromCollections(metadata_db.Collection.find())
    results = {}
    
    starting_design = design.Design()
    for col in collections :
        starting_design.addCollection(col['name'])
        results[col['name']] = {}
        col_fields = []
        for field, data in col['fields'].iteritems() :
            col_fields.append(field)
            results[col['name']][field] = calc_stats(params, statistics[col['name']]['fields'][field])
        starting_design.addFieldsOneCollection(col['name'], col_fields)
        attr = None
        value = 0
        for field, data in results[col['name']].iteritems() :
            if data >= value :
                value = data
                attr = field
        starting_design.addShardKey(col['name'], attr)
        
    ## ----------------------------------------------
    ## STEP 2
    ## Create Workload for passing into cost function
    ## ----------------------------------------------
    wrkld = workload.Workload()
    for rec in metadata_db[constants.COLLECTION_WORKLOAD].find() :
        sessn = workload.Sess()
        if len(rec['operations']) > 0 :
            sessn.startTime = rec['operations'][0]['timestamp']
            sessn.endTime = rec['operations'][len(rec['operations']) - 1]['timestamp']
        for op in rec['operations'] :
            qry = workload.Query()
            qry.collection = op['collection']
            qry.timestamp = op['timestamp']
            if op['type'] == '$insert' :
                qry.type = 'insert'
                print 'insert'
                # No predicate for insert operations
            elif op['type'] == '$query' :
                qry.type = 'select'
                if op['content'][0]['query'] <> None :
                    for k,v in op['content'][0]['query'].iteritems() :
                        if type(v) == 'dict' :
                            qry.predicates[k] = 'range'
                        else :
                            qry.predicates[k] = 'equality'
            elif op['type'] == '$update' :
                qry.type = 'update'
                # todo: add predicates from update queries
            elif op['type'] == '$remove' :
                qry.type = 'delete'
                # todo: add predicates from delete queries
            else :
                qry.type = None
            sessn.queries.append(qry)
        wrkld.addSession(sessn)
    
    cm = costmodel.CostModel(wrkld, {'alpha' : 1.0, 'beta' : 1.0, 'gamma' : 1.0, 'nodes' : 10}, statistics)
    print statistics
    print 'Network Cost: ', cm.networkCost(starting_design)
    print 'Disk Cost: ', cm.diskCost(starting_design)
    print 'Skew Cost: ', cm.skewCost(starting_design)
    
    ## ----------------------------------------------
    ## STEP 3
    ## Execute the LNS design algorithm
    ## ----------------------------------------------
    
## MAIN