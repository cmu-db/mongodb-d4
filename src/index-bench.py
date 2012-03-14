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
import random
import string
import time
from util import *

LOG = logging.getLogger(__name__)

    
def show_results(start, stop, num) :
    print num, 'queries executed'
    elapsed = stop - start
    print elapsed, 'time elapsed'
    avg = elapsed / num
    print avg, 'time per query'
    return None
    
## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s\n%s" % (constants.PROJECT_NAME, constants.PROJECT_URL))
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--queries', type=int, default=1000)
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

    columns = ['key1', 'key2']
    generate_db = conn['synthetic']
    limit = args['queries']
    data_col = 'data'
    value_col = 'values'
    
    ## -----------------------------------------------------
    ## Read in set of values from which to query
    ## -----------------------------------------------------
    values = generate_db[value_col].find()
    key1_values = []
    key2_values = []
    for row in values :
        key1_values.append(row['key1'])
        key2_values.append(row['key2'])
    num_values = len(key1_values)
        
    ## -----------------------------------------------------
    ## Execute Micro-Benchmarks for MongoDB Indexes
    ## -----------------------------------------------------
    print 'Micro-Benchmarking MongoDB Indexes'
    generate_db[data_col].drop_indexes()
    print 'Executing queries with no indexes'
    
    start = time.time()
    for i in range(limit) :
        key = random.randint(0, num_values - 1)
        generate_db[data_col].find({'key1': key1_values[key]})
        generate_db[data_col].find({'key2': key2_values[key]})
    end = time.time()
    show_results(start, end, limit)
    
    print 'Generating indexes'
    generate_db[data_col].ensure_index('key1')
    generate_db[data_col].ensure_index('key2')
    
    print 'Executing benchmarks on covering indexes'
    start = time.time()
    for i in range(limit) :
        key = random.randint(0, num_values - 1)
        generate_db[data_col].find({'key1': key1_values[key]})
        generate_db[data_col].find({'key2': key2_values[key]})
    end = time.time()
    show_results(start, end, limit)
        
    generate_db[data_col].drop_indexes()
## END MAIN