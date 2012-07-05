# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
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
import logging
from pprint import pformat

# Third-Party Dependencies
from inputs.mongodb import parser, sessionizer

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../libs"))

# mongodb-d4
sys.path.append(os.path.join(basedir, ".."))
from abstractconverter import AbstractConverter

import parser
import reconstructor
import sessionizer
from workload import Session
from util import Histogram

LOG = logging.getLogger(__name__)

## ==============================================
## MongoSniff Trace Converter
## ==============================================
class MongoSniffConverter(AbstractConverter):
    
    def __init__(self, metadata_db, dataset_db, fd=None):
        AbstractConverter.__init__(self, metadata_db, dataset_db)
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.fd = fd
        self.no_mongo_parse = False
        self.no_mongo_reconstruct = False
        self.no_mongo_sessionizer = False
        self.mongo_skip = None
        self.op_limit = None
    ## DEF
        
    def loadImpl(self):
        if not self.no_mongo_parse:
            self.parseWorkload(self.fd)
            
        if not self.no_mongo_reconstruct:
            self.reconstructDatabase()
            
        if not self.no_mongo_sessionizer:
            self.sessionizeWorkload()
    ## DEF
    
    ## ----------------------------------------------
    ## WORKLOAD PARSING + LOADING
    ## ----------------------------------------------
    def parseWorkload(self, fd):
        # Create the Parser object that will go to town on our input file 
        p = parser.Parser(self.workload_col, fd)
        
        # Stop on Error
        if self.stop_on_error:
            LOG.info("Will stop processing if invalid input is found")
            p.stop_on_error = True
        # Processing Skip
        if self.mongo_skip:
            LOG.info("Will skip processing the first %d lines" % self.mongo_skip)
            p.op_skip =  self.mongo_skip
        # Processing Limit
        if self.op_limit:
            LOG.info("Will stop reading workload trace after %d operations are processed" % self.op_limit)
            p.op_limit =  self.op_limit
        
        # Clear our existing data
        if self.clean: p.clean()
        
        # Bombs away!
        LOG.info("Processing mongosniff trace input")
        p.process()
        LOG.info("Finishing processing")
        LOG.info("Added %d sessions with %d operations to '%s'" % (\
            p.getSessionCount(), p.getOpCount(), self.workload_col.full_name))
        LOG.info("Skipped Responses: %d" % p.getOpSkipCount())
    ## IF
    
    ## ----------------------------------------------
    ## DATABASE RECONSTRUCTION
    ## ----------------------------------------------
    def reconstructDatabase(self):
        # Create a Reconstructor that will use the WORKLOAD_COL to regenerate
        # the original database and extract a schema catalog.
        r = reconstructor.Reconstructor(self.metadata_db, self.dataset_db)
        
        # Clear our existing data
        if self.clean: r.clean()
        
        # Bombs away!
        r.process()
        LOG.info("Processed %d sessions with %d operations into '%s'" % (\
                 r.getSessionCount(), r.getOpCount(), self.dataset_db.name))
        if self.debug:
            LOG.debug("Skipped Operations: %d" % r.getOpSkipCount())
            LOG.debug("Fixed Operations: %d" % r.getOpFixCount())
            LOG.debug("Collection Sizes:\n%s" % pformat(r.getCollectionCounts()))
    ## DEF
    
    ## ----------------------------------------------
    ## WORKLOAD SESSIONIZATION
    ## ----------------------------------------------
    def sessionizeWorkload(self):
        """

        """
        LOG.info("Sessionizing sample workload")
        
        s = sessionizer.Sessionizer()
        
        # We first feed in all of the operations in for each session
        nextSessId = -1
        origTotal = 0
        origHistogram = Histogram()
        sessions = [ ]
        for sess in self.metadata_db.Session.fetch():
            s.process(sess['session_id'], sess['operations'])
            nextSessId = max(nextSessId, sess['session_id'])
            origHistogram.put(len(sess['operations']))
            origTotal += len(sess['operations'])
            sessions.append(sess)
        ## FOR
        LOG.info("ORIG - Sessions: %d" % origHistogram.getSampleCount())
        LOG.info("ORIG - Avg Ops per Session: %.2f" % (origTotal / float(origHistogram.getSampleCount())))
        
        # Then split them into separate sessions
        s.calculateSessions()
        newTotal = 0
        newHistogram = Histogram()

        # We have to do this because otherwise we will start to process
        # the new sessions that we just inserted... I know...
        deletable = [ ]
        for sess in sessions:
            newSessions = s.sessionize(sess, nextSessId)
            nextSessId += len(newSessions)
            
            # Mark the original session as deletable
            deletable.append(sess)

            # And then add all of our new sessions
            # Count the number of operations so that can see the change
            if self.debug:
                LOG.debug("Split Session %d [%d ops] into %d separate sessions", \
                          sess['session_id'], len(sess['operations']), len(newSessions))
            totalOps = 0
            for newSess in newSessions:
                self.metadata_db.Session.save(newSess)
                newOpCtr = len(newSess['operations'])
                totalOps += newOpCtr
                newHistogram.put(newOpCtr)
                if self.debug:
                    LOG.debug("Session %d -> %d Ops" % (newSess['session_id'], newOpCtr))
            # Make sure that all of our operations end up in a session
            assert len(sess['operations']) == totalOps, \
                "Expected %d operations, but new sessions only had %d" % (len(sess['operations']), totalOps)
            newTotal += totalOps
        ## FOR
        LOG.info("NEW  - Sessions: %d" % newHistogram.getSampleCount())
        LOG.info("NEW  - Avg Ops per Session: %.2f" % (newTotal / float(newHistogram.getSampleCount())))
        if self.debug:
            LOG.debug("NEW - Ops per Session\n%s" % newHistogram)

        self.metadata_db.Session.remove(deletable)

        return
    ## DEF
## CLASS