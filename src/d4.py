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
from pprint import pformat
import sys
import argparse
import logging
import time
from ConfigParser import RawConfigParser

# Third-Party Dependencies
# Third-Party Dependencies
if __name__ == '__channelexec__':
    # Remote execnet invocations won't have a __file__
    basedir = os.getcwd()
else:
    basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../libs"))
sys.path.append(os.path.join(basedir, "multithreaded/"))
import mongokit

# mongodb-d4
import catalog
import workload
from search import Designer
from util import configutil
from util import constants
from util import termcolor
from multi_search import MultiClientDesigner
from messageprocessor import *

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
                                      description="%s - Distributed Document Database Designer" % constants.PROJECT_NAME)
                                      
    # Configuration File Options
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file.')

    # General Processing Options
    agroup = aparser.add_argument_group(termcolor.bold('General Options'))
    agroup.add_argument('--reset', action='store_true',
                         help='Reset the internal catalog databases before processing. ' +
                              'Warning: This will delete all of the collections in the ' +
                              'metadata and workload databases. Use with caution.')
    agroup.add_argument('--no-load', action='store_true',
                        help='Skip loading input files into system. ' +
                             'Use this option if schema statistics and the workload have ' +
                             'already been loaded into the catalog database.')
    agroup.add_argument('--no-search', action='store_true',
                        help='Do not perform a search for a database design.')
    agroup.add_argument('--sess-limit', type=int, metavar='S', default=None,
                        help='Limit the number of sessions to process from the sample workload.')
    agroup.add_argument('--op-limit', type=int, metavar='N', default=None,
                        help='Limit the number of operations to process from the sample workload.')

    # MongoDB Trace Processing Options
    agroup = aparser.add_argument_group(termcolor.bold('MongoDB Workload Processing Options'))
    agroup.add_argument('--mongo', type=str, metavar='FILE',
                        help="Path to the MongoSniff file with the sample workload. Use '-' if you would like to read from stdin")
    agroup.add_argument('--mongo-skip', type=int, metavar='N', default=None,
                        help='Skip the first N lines in the MongoSniff input file.')
    # TODO: These are development option that should be removed
    agroup.add_argument('--no-mongo-parse', action='store_true',
                        help='Skip parsing and loading MongoSniff workload trace file into the internal catalog.'),
    agroup.add_argument('--no-mongo-reconstruct', action='store_true',
                        help='Skip reconstructing the MongoDB database schema after loading.')
    agroup.add_argument('--no-mongo-sessionizer', action='store_true',
                        help='Skip splitting the MongoSniff workload into separate sessions.')

    # MySQL Processing Options
    agroup = aparser.add_argument_group(termcolor.bold('MySQL Workload Processing Options'))
    agroup.add_argument('--mysql', action='store_true',
                        help='Whether to process inputs from MySQL. Use must also define ' +
                             'the database connection parameters in the config file.'),

    # Debugging Options
    agroup = aparser.add_argument_group(termcolor.bold('Debugging Options'))

    agroup.add_argument('--debug', action='store_true',
                         help='Enable debug log messages.')
    # TODO: These are development options that should be removed
    agroup.add_argument('--no-post-process', action='store_true',
                        help='Skip post-processing the workload trace after loading it into ' +
                             'the internal catalog database.')
    agroup.add_argument('--stop-on-error', action='store_true',
                        help='Stop processing when an invalid record is encountered.')

    aparser.add_argument('--output-design', type=str,
                         help='Path to final design file.')
    
    args = vars(aparser.parse_args())

    if args['debug']: LOG.setLevel(logging.DEBUG)
    if args['print_config']:
        print configutil.formatDefaultConfig()
        sys.exit(0)
    
    if not args['config']:
        LOG.error("Missing configuration file")
        print
        aparser.print_usage()
        sys.exit(1)
    LOG.debug("Loading configuration file '%s'" % args['config'])
    config = RawConfigParser()
    configutil.setDefaultValues(config)
    config.read(os.path.realpath(args['config'].name))
    
    ## ----------------------------------------------
    ## Connect to MongoDB
    ## ----------------------------------------------
    hostname = config.get(configutil.SECT_MONGODB, 'host')
    port = config.getint(configutil.SECT_MONGODB, 'port')
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
        if not config.has_option(configutil.SECT_MONGODB, key):
            raise Exception("Missing the configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
        elif not config.get(configutil.SECT_MONGODB, key):
            raise Exception("Empty configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
    ## FOR

    ## ----------------------------------------------
    ## MONGODB DATABASE RESET
    ## ----------------------------------------------
    metadata_db = conn[config.get(configutil.SECT_MONGODB, 'metadata_db')]
    dataset_db = conn[config.get(configutil.SECT_MONGODB, 'dataset_db')]

    if args['reset']:
        LOG.warn("Dropping collections from %s and %s databases" % (metadata_db.name, dataset_db.name))
        for col_name in [metadata_db.Session.collection.name, metadata_db.Collection.collection.name]:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.warn("Dropping %s.%s", metadata_db.name, col_name)
            metadata_db.drop_collection(col_name)
        ## FOR

        for col_name in dataset_db.collection_names():
            if col_name.startswith("system"): continue
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.warn("Dropping %s.%s" % (dataset_db.name, col_name))
            dataset_db.drop_collection(col_name)
        ## FOR
    ## IF
    
    # This designer is only used for input processing
    designer = Designer(config, metadata_db, dataset_db)
    designer.setOptionsFromArguments(args)
    
    start = time.time()
    try:
        ## ----------------------------------------------
        ## STEP 1: INPUT PROCESSING
        ## ----------------------------------------------
        if not (args['no_load'] or args['no_post_process']):
            if not args['mysql']:
                # If the user passed in '-', then we'll read from stdin
                inputFile = args['mongo']
                if not inputFile:
                    if not args['no_load']:
                        LOG.warn("A mongonsiff trace file was not provided. Reading from standard input...")
                    inputFile = "-"
                with open(inputFile, 'r') if inputFile != '-' else sys.stdin as fd:
                    designer.processMongoInput(\
                        fd, \
                        no_load=args['no_load'], \
                        no_post_process=args['no_post_process'], \
                    )
                ## WITH
            else:
                designer.processMySQLInput(\
                    no_load=args['no_load'],\
                    no_post_process=args['no_post_process'], \
                )
        else:
            LOG.warn("Skipping workload trace loading and processing...")
        ## IF

        if args['no_search']:
            LOG.warn("Not performing design search. Halting")
            sys.exit(0)

        ## ----------------------------------------------
        ## STEP 2: Execute the LNS/BB Search design algorithm
        ## ----------------------------------------------
        #import pycallgraph
        #pycallgraph.start_trace()
        # Bombs away!!! Quote from the previous contributors 
        mcd = MultiClientDesigner(config, args)
        mcd.runSearch()
        #try:
            #finalSolution = designer.search()
        #finally:
            ##pycallgraph.make_dot_graph('d4.png')
            #pass
        #LOG.info("Final Solution:\n%s", finalSolution)
    finally:
        stop = time.time()
        LOG.info("Total Time: %.1f sec", (stop - start))

## MAIN

## ==============================================
## EXECNET PROCESSOR
## ==============================================
if __name__ == '__channelexec__':
#import pycallgraph
#import os
#pycallgraph.start_trace()
#pid=os.getpid()
#try:
    mp = MessageProcessor(channel)
    mp.processMessage()
    #finally:
    #    pycallgraph.make_dot_graph("d4-"+str(pid)+".png")
    #    pass
    ## EXEC