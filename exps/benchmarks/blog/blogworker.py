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
import re
import logging
import traceback
import pymongo
from pprint import pprint, pformat

import drivers
from util import *
from runtime import *
from api.abstractworker import AbstractWorker

LOG = logging.getLogger(__name__)

class BlogWorker(AbstractWorker):
    
    def initImpl(self, config, channel):
        # Total number of articles in database
        self.num_articles = int(config["scalefactor"] * constants.NUM_ARTICLES)
        
        # Zipfian distribution on the number of comments & their ratings
        self.commentsZipf = ZipfGenerator(constants.MAX_NUM_COMMENTS, 1.0)
        self.ratingZipf = ZipfGenerator(constants.MAX_COMMENT_RATING, 1.0)
        
        ## ----------------------------------------------
        ## Connect to MongoDB
        ## ----------------------------------------------
        self.conn = None
        try:
            self.conn = pymongo.Connection(config['host'], config['port'])
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (config['host'], config['port']))
            raise
        assert self.conn
        
        if config["reset"]:
            LOG.info("Resetting database '%s'" % constants.DB_NAME)
            self.conn.drop_database(constants.DB_NAME)
        
        pass
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        assert self.conn != None
        
        # The message we're given is a tuple that contains
        # the first and articleIds that we're going to insert, and
        # the list of author names that we'll generate articles from
        firstArticle, lastArticle, authors = msg.data
        
        # Check whether they set the denormalize flag
        if not "denormalize" in config:
            config["denormalize"] = False
        
        LOG.info("Generating %s data" % self.getBenchmarkName())
        articleCtr = 0
        commentCtr = 0
        commentId = self.getWorkerId() * 1000000
        for articleId in xrange(firstArticle, lastArticle):
            titleSize = int(random.gauss(constants.MAX_TITLE_SIZE/2, constants.MAX_TITLE_SIZE/4))
            contentSize = int(random.gauss(constants.MAX_CONTENT_SIZE/2, constants.MAX_CONTENT_SIZE/4))
            
            title = rand.randomString(titleSize)
            slug = list(title.replace(" ", ""))
            for idx in xrange(0, titleSize):
                if random.randint(0, 10) == 0:
                    slug[idx] = "-"
            ## FOR
            slug = "".join(slug)
            articleDate = rand.randomDate(constants.START_DATE, constants.STOP_DATE)
            
            article = {
                "id": articleId,
                "title": title,
                "date": articleDate,
                "author": random.choice(authors),
                "slug": slug,
                "content": rand.randomString(contentSize)
            }

            numComments = self.commentsZipf.next()
            lastDate = articleDate
            for ii in xrange(0, numComments):
                lastDate = random_date(lastDate, constants.STOP_DATE)
                commentAuthor = rand.randomString(int(random.gauss(constants.MAX_AUTHOR_SIZE/2, constants.MAX_AUTHOR_SIZE/4)))
                commentContent = rand.randomString(int(random.gauss(constants.MAX_COMMENT_SIZE/2, constants.MAX_COMMENT_SIZE/4)))
                
                comment = {
                    "id": commentId,
                    "article": articleId,
                    "date": lastDate, 
                    "author": commentAuthor,
                    "comment": commentContent,
                    "rating": int(self.ratingZipf.next())
                }
                commentCtr += 1
                if not config["denormalize"]:
                    self.conn[constants.DB_NAME][constants.COMMENT_COLL].insert(comment)
                else:
                    if not "comments" in article:
                        article["comments"] = [ ]
                    article["comments"].append(comment)
                commentId += 1
            ## FOR (comments)

            # Always insert the article
            self.conn[constants.DB_NAME][constants.ARTICLE_COLL].insert(article)
            articleCtr += 1
            if articleCtr % 1000 == 0 :
                LOG.info("ARTICLE: %6d / %d" % (articleCtr, (lastArticle - firstArticle))
        ## FOR (articles)
        
        LOG.info("# of ARTICLES: %d" % articleCtr)
        LOG.info("# of COMMENTS: %d" % commentCtr)
        
    ## DEF
        
    def executeImpl(self, config, channel, msg):
        assert self.conn != None
    
        return (results)
    ## DEF    
## CLASS