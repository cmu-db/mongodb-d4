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
from reconstructor import Reconstructor
from sessionizer import Sessionizer
from normalizer import Normalizer
from dependencyfinder import DependencyFinder

from workload import Session
from util import Histogram
from util import constants

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
        self.no_mongo_aggregate_fix = False
        self.no_mongo_reconstruct = False
        self.no_mongo_sessionizer = False
        self.no_mongo_dependencies = False
        self.no_mongo_normalize = False
        self.random_sessionizer = False
        self.mongo_skip = None
        self.sess_limit = None
        self.op_limit = None
    ## DEF
        
    def loadImpl(self):
        if not self.no_mongo_parse:
            self.parseWorkload(self.fd)
            
        if not self.no_mongo_reconstruct:
            self.reconstructDatabase()
        # NOTE: We have to sessionize the workload before we normalize it!!
        if not self.no_mongo_sessionizer:
            self.sessionizeWorkload()
        if not self.no_mongo_normalize:
            self.normalizeDatabase()

#        self.printAllOperations()
#        self.countDocs()
#        self.printAllDocs()
        
#        self.printAllOperations()
#        self.printAllDocs()
#        self.countDocs()
#        exit("CUPCAKE")
    ## DEF
    
    def postProcessImpl(self):
        if not self.no_mongo_dependencies:
            self.findDependencies()

    ## DEF
    
    ## ----------------------------------------------
    ## NORMALIZE DATASET AND RE-CONSTRUCT OPERATIONS
    ## ----------------------------------------------
    def normalizeDatabase(self):
        """
        If a collection has a field which contains several fields,
        we extract those fields and make a new collection out of it
        """
        n = Normalizer(self.metadata_db, self.dataset_db)

        # Bombs away!
        LOG.info("Processing mongodb dataset normalization")
        n.process()
        LOG.info("Finshing normalization")
    ## DEF
    
    ## ----------------------------------------------
    ## FIND US SOME DEPENDENCIES!
    ## ----------------------------------------------
    def findDependencies(self):
        """
        If a collection has a field which contains several fields,
        we extract those fields and make a new collection out of it
        """
        d = DependencyFinder(self.metadata_db, self.dataset_db)

        # Bombs away!
        LOG.info("Searching for depedencies")
        d.process()
        LOG.info("Finshing depedency search")
    ## DEF

    ## ----------------------------------------------
    ## WORKLOAD PARSING + LOADING
    ## ----------------------------------------------
    def parseWorkload(self, fd):
        # Create the Parser object that will go to town on our input file 
        p = parser.Parser(self.metadata_db, fd, skipSaltSearch=self.no_mongo_aggregate_fix)
        
        # Stop on Error
        if self.stop_on_error:
            LOG.info("Will stop processing if invalid input is found")
            p.stop_on_error = True
        # Processing Skip
        if self.mongo_skip:
            LOG.info("Will skip processing the first %d lines" % self.mongo_skip)
            p.op_skip =  self.mongo_skip
        # Processing Limit
        if self.sess_limit:
            LOG.info("Will stop reading workload trace after %d Sessions are processed", self.sess_limit)
            p.sess_limit =  self.sess_limit
        if self.op_limit:
            LOG.info("Will stop reading workload trace after %d Operations are processed", self.op_limit)
            p.op_limit =  self.op_limit
        
        # Clear our existing data
        if self.clean: p.clean()
        
        # Bombs away!
        LOG.info("Processing mongosniff trace input")
        p.process()
        LOG.info("Finishing processing")
        LOG.info("Added %d sessions with %d operations to '%s'" % (\
            p.getSessionCount(), p.getOpCount(), self.metadata_db.collection.full_name))
        LOG.info("Skipped Responses: %d" % p.getOpSkipCount())
    ## IF
    
    ## ----------------------------------------------
    ## DATABASE RECONSTRUCTION
    ## ----------------------------------------------
    def reconstructDatabase(self):
        # Create a Reconstructor that will use the WORKLOAD_COL to regenerate
        # the original database and extract a schema catalog.
        r = Reconstructor(self.metadata_db, self.dataset_db)
        
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
            Split the Sessions based on the gap between operation times
        """
        LOG.info("Sessionizing sample workload")
        
        s = Sessionizer(self.metadata_db, randomize=self.random_sessionizer)
        
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
        avg_ops = 0 if origHistogram.getSampleCount() == 0 else (origTotal / float(origHistogram.getSampleCount()))
        LOG.info("BEFORE Sessionization\n" +
                 "  # of Sessions: %d\n" +
                 "  Avg Ops per Session: %.2f", \
                 origHistogram.getSampleCount(), \
                 avg_ops)
        
        # Then split them into separate sessions
        s.calculateSessions()
        newTotal = 0
        newHistogram = Histogram()

        # We have to do this because otherwise we will start to process
        # the new sessions that we just inserted... I know...
        for sess in sessions:
            newSessions = s.sessionize(sess, nextSessId)
            nextSessId += len(newSessions)
            
            # And then add all of our new sessions
            # Count the number of operations so that can see the change
            if self.debug:
                LOG.debug("Split Session %d [%d ops] into %d separate sessions", \
                          sess['session_id'], len(sess['operations']), len(newSessions))
            totalOps = 0
            for newSess in newSessions:
                try:
                    newSess.save()
                except:
                    LOG.error("Unexpected error when saving new Session\n%s", pformat(newSess))
                    raise
                newOpCtr = len(newSess['operations'])
                totalOps += newOpCtr
                newHistogram.put(newOpCtr)
                if self.debug:
                    LOG.debug("Session %d -> %d Ops" % (newSess['session_id'], newOpCtr))
            # Make sure that all of our operations end up in a session
            assert len(sess['operations']) == totalOps, \
                "Expected %d operations, but new sessions only had %d" % (len(sess['operations']), totalOps)
            newTotal += totalOps
            
            # Mark the original session as deletable
            # deletable.append(sess)
            sess.delete()
        ## FOR
        avg_ops = 0 if origHistogram.getSampleCount() == 0 else (origTotal / float(origHistogram.getSampleCount()))
        LOG.info("AFTER Sessionization\n" +
                 "  # of Sessions: %d\n" +
                 "  Avg Ops per Session: %.2f", \
                 newHistogram.getSampleCount(), \
                 avg_ops)
        if self.debug:
            LOG.debug("Ops per Session\n%s" % newHistogram)
            
        return
    ## DEF
## CLASS