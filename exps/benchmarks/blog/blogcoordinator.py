# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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

import sys
import os
import string
import random
import logging
from pprint import pprint, pformat

import constants
from util import *
from api.abstractcoordinator import AbstractCoordinator
from api.message import *

LOG = logging.getLogger(__name__)

class BlogCoordinator(AbstractCoordinator):
    DEFAULT_CONFIG = [
        ("commentsperarticle","Number of comments per article",100),
        ("experiment", "What type of experiment to execute. Valid values = %s" % constants.EXP_ALL, constants.EXP_DENORMALIZATION),
        ("sharding", "Sharding experiment configuration type. Valid values = %s" % constants.SHARDEXP_ALL, constants.SHARDEXP_SINGLE),
        ("indexes", "Indexing experiment configuration type. Valid values = %s" % constants.INDEXEXP_8020, constants.INDEXEXP_9010),
        ("denormalize", "If set to true, then the COMMENTS are denormalized into ARTICLES", False),
        ("skew","Determine the percentage of the articles to be retreived with uniform distribution. The rest will be retreived with Zipfian distribution",0.3)
,    ]
    
    def benchmarkConfigImpl(self):
        return self.DEFAULT_CONFIG
    ## DEF
    
    def initImpl(self, config, channels):
        self.num_articles = int(config['default']["scalefactor"] * constants.NUM_ARTICLES)
        
        config[self.name]["denormalize"] = (config[self.name]["denormalize"] == "True")
        
        # Experiment Type
        config[self.name]["experiment"] = config[self.name]["experiment"].strip()
        if not config[self.name]["experiment"] in constants.EXP_ALL:
            raise Exception("Invalid experiment code '%s'" % config[self.name]["experiment"])
        
        # Sharding Experiment Configuration
        if config[self.name]["experiment"] == constants.EXP_SHARDING:
            assert "sharding" in config[self.name]
            config[self.name]["sharding"] = int(config[self.name]["sharding"])
            if not config[self.name]["sharding"] in constants.SHARDEXP_ALL:
                raise Exception("Invalid sharding experiment configuration type '%d'" % config[self.name]["sharding"])
            
        # Indexing Experiment Configuration
        if config[self.name]["experiment"] == constants.EXP_INDEXING:
            assert "indexes" in config[self.name]
            config[self.name]["indexes"] = int(config[self.name]["indexes"])
            #if not config[self.name]["indexes"] in constants.INDEXEXP_ALL:
            #    raise Exception("Invalid indexing experiment configuration type '%d'" % config[self.name]["indexes"])
        
        # Check whether they set the denormalize flag
        if not "denormalize" in config[self.name]:
            config[self.name]["denormalize"] = False
        
        # Precompute our blog article authors
        # The list of authors have names authorname1 authorname2 ... etc authornameN so
        # we can use Zipfian later for the same names
        self.authors = [ ]
        #for i in xrange(0, constants.NUM_AUTHORS):
        #    #authorSize = constants.AUTHOR_NAME_SIZE
        #    self.authors.append("authorname"+str(i))
        ## FOR
        
        #Precompute our discrete Dates (change is only by day - times stay the same)
        #self.dates = [ ]
        #for i in xrange(STOP_DATE,START_DATE,-3600):
        #    self.dates.append(i)
        #
        # Get the current max commentId
        # Figure out how many comments already exist in the database
        config[self.name]["maxCommentId"] = -1
        
        if not config['default']["reset"] and config[self.name]["denormalize"]:
            #assert False
            pass
        elif not config['default']["reset"]:
            LOG.debug("Calculating maxCommentId for %s" % constants.COMMENT_COLL)
            db = self.conn[config['default']["dbname"]]
            if db[constants.COMMENT_COLL].count() > 0:
                result = db[constants.COMMENT_COLL].find({}, {"id":1}).sort("id", -1).limit(1)[0]
                config[self.name]["maxCommentId"] = result["id"]
        ## IF
        
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("# of Articles:   %d" % self.num_articles)
            LOG.debug("Experiment Type: %s" % config[self.name]["experiment"])
            LOG.debug("Sharding Type:   %s" % config[self.name]["sharding"])
            LOG.debug("Denormalize:     %s" % config[self.name]["denormalize"])
            LOG.debug("Indexing Type:   %s" % config[self.name]["indexes"])
            LOG.debug("MaxCommentId:    %s" % config[self.name]["maxCommentId"])
        
        return
    ## DEF
    
    def loadImpl(self, config, channels):
        procs = len(channels)
        articleRange = [ ]
        articlesPerChannel = self.num_articles / procs
        first = 0
        for i in range(len(channels)):
            last = first + articlesPerChannel
            LOG.info("Loading %s [%d - %d) on Worker #%d" % (constants.ARTICLE_COLL, first, last, i))
            sendMessage(MSG_CMD_LOAD, (self.authors, config[self.name]["maxCommentId"]), channels[i])
            first = last
    ## DEF

## CLASS