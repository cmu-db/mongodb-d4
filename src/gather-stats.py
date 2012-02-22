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
    schema_db = conn[cparser.get(config.SECT_MONGODB, 'schema_db')]
    dataset_db = conn[cparser.get(config.SECT_MONGODB, 'dataset_db')]
    workload_db = conn[cparser.get(config.SECT_MONGODB, 'workload_db')]
    
    for rec in workload_db['sessions'].find() :
        print '----------------------------------------'
        for op in rec['operations'] :
            if op['type'] == '$delete' :
                for content in op['content'] :
                    if content <> {} :
                        for k,v in content.iteritems() :
                            print k, v
            elif op['type'] == '$insert' :
                for content in op['content'] :
                    if content <> {} :
                        print 'Insert {',
                        for k,v in content.iteritems() :
                            print k, '-', v, '|',
                        print '}'
            elif op['type'] == '$query' :
                '''
                todo - extract query structure from string
                '''
                pass
            elif op['type'] == '$update' :
                length = len(op['content'])
                i = 0
                while i < length :
                    print i
                    i += 1
## END MAIN