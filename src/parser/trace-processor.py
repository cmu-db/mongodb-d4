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
from __future__ import with_statement

import os
import sys
import fileinput
import time
import argparse
import logging
from pprint import pformat
from pymongo import Connection

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# MongoDB-Designer
sys.path.append(os.path.join(basedir, ".."))
import workload_info
import parser
import reconstructor
from traces import *
from workload import sessionizer
from util.histogram import Histogram

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

## ==============================================
## DEFAULT VALUES
## you can specify these with args
## TODO: These should come from util.config
## ==============================================
METADATA_DB = "metadata"
WORKLOAD_COLLECTION = constants.COLLECTION_WORKLOAD
SCHEMA_COL = constants.COLLECTION_SCHEMA
RECREATED_DB = "dataset"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='MongoSniff Trace Process')
    aparser.add_argument('--host', default=DEFAULT_HOST,
                         help='hostname of machine running mongo server')
    aparser.add_argument('--port', type=int, default=DEFAULT_PORT,
                         help='port to connect to')
    aparser.add_argument('--metadata-db', default=METADATA_DB,
                         help='The database used to store the metadata extracted from the sample workload file.')
    aparser.add_argument('--workload-col', default=WORKLOAD_COLLECTION,
                         help='The collection where you want to store the traces', )
    aparser.add_argument('--schema-col', default=SCHEMA_COL,
                         help='The collection used for the schema catalog in the metadata database.')
    aparser.add_argument('--recreated-db', default=RECREATED_DB,
                         help='The database used to stored the recreated database derived from the sample workload.', )
    aparser.add_argument('--clean', action='store_true',
                         help='Remove all documents in each database before processing is started.')
                         
    aparser.add_argument('--no-load', action='store_true',
                         help='Skip parsing and loading workload from file.')
    aparser.add_argument('--no-reconstruct', action='store_true',
                         help='Skip reconstructing the database schema after loading.')
    aparser.add_argument('--no-sessionizer', action='store_true',
                         help='Skip splitting the sample workload into separate sessions.')
                         
    # Debugging Options
    aparser.add_argument('--skip', type=int, default=None,
                         help='Skip the first N lines in the input file')
    aparser.add_argument('--limit', type=int, default=None,
                         help='Limit the number of operations to process')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop processing when an invalid line is reached')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')

    args = vars(aparser.parse_args())
    if args['debug']:
        LOG.setLevel(logging.DEBUG)
        parser.LOG.setLevel(logging.DEBUG)
        reconstructor.LOG.setLevel(logging.DEBUG)
        sessionizer.LOG.setLevel(logging.DEBUG)

    LOG.info("..:: MongoSniff Trace Processor ::..")
    LOG.debug("Server: %(host)s:%(port)d / Storage: %(metadata_db)s.%(workload_col)s" % args)

    # initialize connection to MongoDB
    LOG.debug("Connecting to MongoDB at %s:%d" % (args['host'], args['port']))
    connection = Connection(args['host'], args['port'])
    
    # The WORKLOAD collection is where we stores sessions+operations
    workload_col = connection[args['metadata_db']][args['workload_col']]
    assert workload_col, "Invalid WORKLOAD collection %s.%s" % (args['metadata_db'], args['workload_col'])
    
    # The RECREATED database will contain the database derived from the
    # keys+values used in the sample workload's operations
    recreated_db = connection[args['recreated_db']]
    assert recreated_db, "Invalid RECREATED database %s" % (args['recreated_db'])

    # The SCHEMA collection is where we will store the metadata information that
    # we will derive from the RECREATED database
    schema_col = connection[args['metadata_db']][args['schema_col']]
    assert schema_col, "Invalid SCHEMA collection %s.%s" % (args['metadata_db'], args['schema_col'])
    
    ## ----------------------------------------------
    ## WORKLOAD PARSING + LOADING
    ## ----------------------------------------------
    if not args['no_load']:
        with sys.stdin as fd:
            # Create the Parser object that will go to town on our input file 
            p = parser.Parser(workload_col, fd)
            
            # Stop on Error
            if args['stop_on_error']:
                LOG.warn("Will stop processing if invalid input is found")
                p.stop_on_error = True
            # Processing Skip
            if args['skip']:
                LOG.warn("Will skip processing the first %d lines" % args['skip'])
                p.op_skip =  args['skip']
            # Processing Limit
            if args['limit']:
                LOG.warn("Will stop processing after %d operations are processed" % args['limit'])
                p.op_limit =  args['limit']
            
            # Clear our existing data
            if args['clean']: p.clean()
            
            # Bombs away!
            LOG.info("Processing mongosniff input")
            p.process()
            LOG.info("Finishing processing")
            LOG.info("Added %d sessions with %d operations to '%s'" % (\
                p.getSessionCount(), p.getOpCount(), workload_col.full_name))
            LOG.info("Skipped Responses: %d" % p.getOpSkipCount())
        ## WITH
    ## IF
    
    ## ----------------------------------------------
    ## DATABASE RECONSTRUCTION
    ## ----------------------------------------------
    if not args['no_reconstruct']:
        # Create a Reconstructor that will use the WORKLOAD_COL to regenerate
        # the original database and extract a schema catalog.
        r = reconstructor.Reconstructor(workload_col, recreated_db, schema_col)
        
        # Clear our existing data
        if args['clean']: r.clean()
        
        # Bombs away!
        r.process()
        LOG.info("Processed %d sessions with %d operations into '%s'" % (\
                 r.getSessionCount(), r.getOpCount(), recreated_db.name))
        LOG.info("Skipped Operations: %d" % r.getOpSkipCount())
        LOG.info("Fixed Operations: %d" % r.getOpFixCount())
        LOG.info("Collection Sizes:\n%s" % pformat(r.getCollectionCounts()))
    ## IF
    
    ## ----------------------------------------------
    ## WORKLOAD SESSIONIZATION
    ## ----------------------------------------------
    if not args['no_sessionizer']:
        LOG.info("Sessionizing sample workload")
        
        s = sessionizer.Sessionizer()
        
        # We first feed in all of the operations in for each session
        nextSessId = -1
        origTotal = 0
        origHistogram = Histogram()
        for sess in workload_col.find():
            s.process(sess['session_id'], sess['operations'])
            nextSessId = max(nextSessId, sess['session_id'])
            origHistogram.put(len(sess['operations']))
            origTotal += len(sess['operations'])
        ## FOR
        LOG.info("ORIG - Sessions: %d" % origHistogram.getSampleCount())
        LOG.info("ORIG - Avg Ops per Session: %.2f" % (origTotal / float(origHistogram.getSampleCount())))
        
        # Then split them into separate sessions
        s.calculateSessions()
        newTotal = 0
        newHistogram = Histogram()
        for sess in workload_col.find():
            newSessions = s.sessionize(sess, nextSessId)
            nextSessId += len(newSessions)
            
            # XXX: Mark the original session as 'deletable'
            sess['deletable'] = True
            # sess.save()
            
            # And then add all of our new sessions
            LOG.info("Split Session %d [%d ops] into %d separate sessions" % (sess['session_id'], len(sess['operations']), len(newSessions)))
            # workload_col.save(newSessions)
            
            # Count the number of operations so that can see the change
            totalOps = 0
            for newSess in newSessions:
                newOpCtr = len(newSess['operations'])
                totalOps += newOpCtr
                newHistogram.put(newOpCtr)
                LOG.debug("Session %d -> %d Ops" % (newSess['session_id'], newOpCtr))
            # Make sure that all of our operations end up in a session
            assert len(sess['operations']) == totalOps, \
                "Expected %d operations, but new sessions only had %d" % (len(sess['operations']), totalOps)
            newTotal += totalOps
        ## FOR
        LOG.info("NEW  - Sessions: %d" % newHistogram.getSampleCount())
        LOG.info("NEW  - Avg Ops per Session: %.2f" % (newTotal / float(newHistogram.getSampleCount())))
        LOG.info("NEW - Ops per Session\n%s" % newHistogram)
            
    ## IF
    
    # Print out some information when parsing finishes
    #if args['debug']:
        #workload_info.print_stats(args['host'], args['port'], args['metadata_db'], args['workload_col'])
    
## MAIN


    
