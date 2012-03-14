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
from util import *

LOG = logging.getLogger(__name__)

def string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def int_generator(min=0, max=1000000000):
    return random.randint(min, max)

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
    generate_db = conn['synthetic']
    long_string = string_generator(10000)
    
    ## -----------------------------------------------------
    ## Generate Synthetic Data for Micro-Benchmarking
    ## -----------------------------------------------------
    print 'Initializing'
    data_col = 'data'
    value_col = 'values'
    generate_db[data_col].remove()
    generate_db[value_col].remove()
    print 'Begin generating synthetic data'
    for i in range(10000000) :
        doc = {}
        doc['key1']  = int_generator()
        doc['key2'] = string_generator(50)
        doc['key3'] = long_string
        doc['key4'] = long_string
        generate_db[data_col].insert(doc)
        if random.randint(1,25) == 1 :
            val_doc = {}
            val_doc['key1'] = doc['key1']
            val_doc['key2'] = doc['key2']
            generate_db[value_col].insert(val_doc)
        if i % 10000 == 0 :
            print i
## END MAIN