#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011 by Brown University
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------
from __future__ import division

import os
import sys
import argparse
import logging
import pymongo
import math
import itertools
import json
from pprint import pprint
from ConfigParser import SafeConfigParser

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../libs"))
import mongokit

# MongoDB-Designer
import catalog
import designer
import workload
import costmodel

from util import *

LOG = logging.getLogger(__name__)

   
    
## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s\n%s" % (constants.PROJECT_NAME, constants.PROJECT_URL))
                                      
    # Configuration File Options
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file used by %s' % constants.PROJECT_NAME)

    # MongoSniff Input File
    aparser.add_argument('--mongosniff', type=str,
                         help='Path to the MongoSniff file with the sample workload')

    # MySQL Processing Options
    aparser.add_argument('--mysql', action='store_true',
                         help='mysql', 'Whether to process inputs from MySQL'),

    # Designer Options
    for key,desc,default in Designer.DEFAULT_CONFIG:
        if type(default) == bool:
            aparser.add_argument('--%s' % key, action='store_true', help=desc)
    ## FOR

    # Debugging Options
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: LOG.setLevel(logging.DEBUG)
    if args['print_config']:
        print config.makeDefaultConfig()
        sys.exit(0)
    
    if not args['config']:
        logging.error("Missing configuration file")
        print
        aparser.print_help()
        sys.exit(1)
    LOG.debug("Loading configuration file '%s'" % args['config'])
    cparser = SafeConfigParser()
    cparser.read(os.path.realpath(args['config'].name))
    config.setDefaultValues(cparser)
    
    ## ----------------------------------------------
    ## Connect to MongoDB
    ## ----------------------------------------------
    hostname = cparser.get(config.SECT_MONGODB, 'host')
    port = cparser.getint(config.SECT_MONGODB, 'port')
    assert hostname
    assert port
    try:
        conn = mongokit.Connection(host=hostname, port=port)
    except:
        LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
        raise
    ## Register our objects with MongoKit
    conn.register([ catalog.Collection, workload.Session, workload.Stats ])

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

    designer = Designer(cparser, metadata_db, dataset_db)
    designer.setOptionsFromArguments(args)
    
    ## ----------------------------------------------
    ## STEP 1: INPUT PROCESSING
    ## ----------------------------------------------
    if not args['mysql']:
        designer.processMongoInput()
    else:
        designer.processMySQLInput()

    ## ----------------------------------------------
    ## STEP 2: INITIAL SOLUTION
    ## ----------------------------------------------
    designer.generateInitialSolution()

    
    ## -------------------------------------------------
    ## STEP 4
    ## Instantiate cost model, determine upper bound from starting design
    ## -------------------------------------------------
    alpha = cparser.getfloat(config.SECT_COSTMODEL, 'weight_network')
    beta = cparser.getfloat(config.SECT_COSTMODEL, 'weight_disk')
    gamma = cparser.getfloat(config.SECT_COSTMODEL, 'weight_skew')
    cluster_nodes = cparser.getint(config.SECT_CLUSTER, 'nodes')
    memory = cparser.getint(config.SECT_CLUSTER, 'node_memory')
    skews = cparser.getint(config.SECT_COSTMODEL, 'time_intervals')
    address_size = cparser.getint(config.SECT_COSTMODEL, 'address_size')
    config_params = {'alpha' : alpha, 'beta' : beta, 'gamma' : gamma, 'nodes' : cluster_nodes, 'max_memory' : memory, 'skew_intervals' : skews, 'address_size' : address_size}
    cm = costmodel.CostModel(wrkld, config_params, statistics)
    upper_bound = cm.overallCost(starting_design)
    
    ## ----------------------------------------------
    ## STEP 5
    ## Instantiate and populate the design candidates
    ## ----------------------------------------------
    dc = designcandidate.DesignCandidate()
    collections = metadata_db.Collection.find()
    for col in collections :
        # addCollection(self, collection, indexKeys, shardKeys, denorm)
        
        # deal with shards
        shardKeys = statistics[col['name']]['interesting']
        
        # deal with indexes
        indexKeys = [[]]
        for o in range(1, len(statistics[col['name']]['interesting']) + 1) :
            for i in itertools.combinations(statistics[col['name']]['interesting'], o) :
                indexKeys.append(i)
                    
        # deal with de-normalization
        denorm = []
        for k,v in col['fields'].iteritems() :
            if v['parent_col'] <> '' :
                if v['parent_col'] not in denorm :
                    denorm.append(v['parent_col'])
        dc.addCollection(col['name'], indexKeys, shardKeys, denorm)
    
    ## ----------------------------------------------
    ## STEP 6
    ## Execute the LNS/BB Search design algorithm
    ## ----------------------------------------------
    bb = bbsearch.BBSearch(dc, cm, starting_design, upper_bound, 10)
    solution = bb.solve()
    
    solutions['final'] = solution.toDICT()
    print json.dumps(solutions, sort_keys=False, indent=4)
## MAIN