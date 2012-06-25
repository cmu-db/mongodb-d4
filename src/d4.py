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
from __future__ import with_statement

import os
import sys
import argparse
import logging
import itertools
import json
from ConfigParser import SafeConfigParser

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../libs"))
import mongokit

# mongodb-d4
import catalog
from designer import Designer
import workload
import costmodel

from util import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

LOG = logging.getLogger(__name__)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s - Distributed Document Database Designer" % (constants.PROJECT_NAME))
                                      
    # Configuration File Options
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file.')
    aparser.add_argument('--reset', action='store_true',
                         help='Reset the metadata and workload database before processing.')


    # MongoDB Trace Processing Options
    agroup = aparser.add_argument_group('MongoDB Processing')
    agroup.add_argument('--mongo', type=str, metavar='FILE',
                        help="Path to the MongoSniff file with the sample workload. Use '-' if you would like to read from stdin")
    agroup.add_argument('--no-mongo-parse', action='store_true',
                        help='Skip parsing and loading MongoDB workload from file.'),
    agroup.add_argument('--no-mongo-reconstruct', action='store_true',
                        help='Skip reconstructing the MongoDB database schema after loading.')
    agroup.add_argument('--no-mongo-sessionizer', action='store_true',
                        help='Skip splitting the MongoDB workload into separate sessions.')
    agroup.add_argument('--mongo-skip', type=int, metavar='N', default=None,
                         help='Skip the first N lines in the MongoSniff input file.')
    agroup.add_argument('--mongo-limit', type=int, metavar='N', default=None,
                         help='Limit the number of operations to process in the MongoSniff input file.')

    # MySQL Processing Options
    agroup = aparser.add_argument_group('MySQL Processing')
    agroup.add_argument('--mysql', action='store_true',
                         help='Whether to process inputs from MySQL. Use must also define ' +
                              'the database connection parameters in the config file.'),

    # Debugging Options
    agroup = aparser.add_argument_group('Debugging Options')
    agroup.add_argument('--stop-on-error', action='store_true',
                         help='Stop processing when an invalid input trace is reached.')
    agroup.add_argument('--debug', action='store_true',
                         help='Enable debug log messages.')
    args = vars(aparser.parse_args())

    if args['debug']: LOG.setLevel(logging.DEBUG)
    if args['print_config']:
        print config.makeDefaultConfig()
        sys.exit(0)
    
    if not args['config']:
        LOG.error("Missing configuration file")
        print
        aparser.print_usage()
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

    ## ----------------------------------------------
    ## MONGODB DATABASE RESET
    ## ----------------------------------------------
    metadata_db = conn[cparser.get(config.SECT_MONGODB, 'metadata_db')]
    dataset_db = conn[cparser.get(config.SECT_MONGODB, 'dataset_db')]
    if args['reset']:
        for col in metadata_db.collection_names():
            if col.startswith("system"): continue
            LOG.warn("Dropping %s.%s" % (metadata_db.name, col))
            metadata_db.drop_collection(col)
        for col in dataset_db.collection_names():
            if col.startswith("system"): continue
            LOG.warn("Dropping %s.%s" % (dataset_db.name, col))
            dataset_db.drop_collection(col)
    ## IF

    designer = Designer(cparser, metadata_db, dataset_db)
    designer.setOptionsFromArguments(args)
    
    ## ----------------------------------------------
    ## STEP 1: INPUT PROCESSING
    ## ----------------------------------------------
    if not args['mysql']:
        # If the user passed in '-', then we'll read from stdin
        inputFile = args['mongosniff']
        if not inputFile:
            LOG.warn("A monognsiff trace file was not provided. Reading from standard input")
            inputFile = "-"
        with open(inputFile, 'r') if inputFile != '-' else sys.stdin as fd:
            designer.processMongoInput(fd)
    else:
        designer.processMySQLInput()

    ## ----------------------------------------------
    ## STEP 2: INITIAL SOLUTION
    ## ----------------------------------------------
    designer.generateInitialSolution()

    ## ----------------------------------------------
    ## STEP 5
    ## Instantiate and populate the design candidates
    ## ----------------------------------------------

    
    ## ----------------------------------------------
    ## STEP 6
    ## Execute the LNS/BB Search design algorithm
    ## ----------------------------------------------
    bb = bbsearch.BBSearch(dc, cm, starting_design, upper_bound, 10)
    solution = bb.solve()
    
    solutions['final'] = solution.toDICT()
    print json.dumps(solutions, sort_keys=False, indent=4)
## MAIN