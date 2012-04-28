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

import constants
from util import *
from api.abstractworker import AbstractWorker
from api.message import *

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
            self.conn = pymongo.Connection(config['host'], int(config['port']))
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (config['host'], config['port']))
            raise
        assert self.conn
        self.db = self.conn[constants.DB_NAME]
        
        if config["reset"]:
            LOG.info("Resetting database '%s'" % constants.DB_NAME)
            self.conn.drop_database(constants.DB_NAME)
        
        # Always drop the indexes
        self.db[constants.ARTICLE_COLL].drop_indexes()
        self.db[constants.COMMENT_COLL].drop_indexes()
        
        # Sharding Key
        if config["experiment"] == 1:
            self.articleZipf = ZipfGenerator(self.num_articles, 1.0)
            self.db[constants.ARTICLE_COLL].create_index([("id", pymongo.ASCENDING)])
            
        # Denormalization
        elif config["experiment"] == 2:
            # We need an index on ARTICLE
            self.db[constants.ARTICLE_COLL].create_index([("id", pymongo.ASCENDING)])
            # And if we're not denormalized, on COMMENTS as well
            if not config["denormalize"]:
                self.db[constants.COMMENT_COLL].create_index([("article", pymongo.ASCENDING)])
                
        # Indexing
        elif config["experiment"] == 3:
            # Nothing
            if config["indexes"] == 0:
                pass
            # Regular Index
            elif config["indexes"] == 1:
                self.db[constants.ARTICLE_COLL].create_index([("article", pymongo.ASCENDING)])
            # Cover Index
            elif config["indexes"] == 2:
                self.db[constants.ARTICLE_COLL].create_index([("id", pymongo.ASCENDING), \
                                                              ("date", pymongo.ASCENDING), \
                                                              ("author", pymongo.ASCENDING)])
            else:
                raise Exception("Unexpected index configuration type %d" % config["indexes"])
        else:
            raise Exception("Unexpected experiment type %d" % config["experiment"])
        
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        assert self.conn != None
        
        # The message we're given is a tuple that contains
        # the first and articleIds that we're going to insert, and
        # the list of author names that we'll generate articles from
        firstArticle, lastArticle, authors = msg.data
        
        LOG.info("Generating %s data" % self.getBenchmarkName())
        articleCtr = 0
        commentCtr = 0
        commentId = self.getWorkerId() * 1000000
        
        ## ----------------------------------------------
        ## LOAD ARTICLES
        ## ----------------------------------------------
        articlesBatch = [ ]
        commentsBatch = [ ]
        for articleId in xrange(firstArticle, lastArticle):
            titleSize = int(random.gauss(constants.MAX_TITLE_SIZE/2, constants.MAX_TITLE_SIZE/4))
            contentSize = int(random.gauss(constants.MAX_CONTENT_SIZE/2, constants.MAX_CONTENT_SIZE/4))
            
            title = randomString(titleSize)
            slug = list(title.replace(" ", ""))
            if len(slug) > 64: slug = slug[:64]
            for idx in xrange(0, len(slug)):
                if random.randint(0, 10) == 0:
                    slug[idx] = "-"
            ## FOR
            slug = "".join(slug)
            articleDate = randomDate(constants.START_DATE, constants.STOP_DATE)
            
            article = {
                "id": articleId,
                "title": title,
                "date": articleDate,
                "author": random.choice(authors),
                "slug": slug,
                "content": randomString(contentSize)
            }

            ## ----------------------------------------------
            ## LOAD COMMENTS
            ## ----------------------------------------------
            numComments = self.commentsZipf.next()
            lastDate = articleDate
            for ii in xrange(0, numComments):
                lastDate = randomDate(lastDate, constants.STOP_DATE)
                commentAuthor = randomString(int(random.gauss(constants.MAX_AUTHOR_SIZE/2, constants.MAX_AUTHOR_SIZE/4)))
                commentContent = randomString(int(random.gauss(constants.MAX_COMMENT_SIZE/2, constants.MAX_COMMENT_SIZE/4)))
                
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
                    commentsBatch.append(comment)
                else:
                    if not "comments" in article:
                        article["comments"] = [ ]
                    article["comments"].append(comment)
                commentId += 1
            ## FOR (comments)

            # Always insert the article
            articlesBatch.append(article)
            articleCtr += 1
            if self.debug and articleCtr % 100 == 0 :
                LOG.debug("ARTICLE: %6d / %d" % (articleCtr, (lastArticle - firstArticle)))
                self.db[constants.ARTICLE_COLL].insert(articlesBatch)
                if len(commentsBatch) > 0:
                    self.db[constants.COMMENT_COLL].insert(commentsBatch)
                articlesBatch = [ ]
                commentsBatch = [ ]
        ## FOR (articles)
        if len(articlesBatch) > 0:
            self.db[constants.ARTICLE_COLL].insert(articlesBatch)
            self.db[constants.COMMENT_COLL].insert(commentsBatch)
        
        LOG.info("# of ARTICLES: %d" % articleCtr)
        LOG.info("# of COMMENTS: %d" % commentCtr)
    ## DEF
    
    def next(self, config):
        assert "experiment" in config
        
        # It doesn't matter what we pick, so we'll just 
        # return the name of the experiment
        txnName = "exp%02d" % config["experiment"]
        params = None
        
        # Sharding Key
        if config["experiment"] == 1:
            assert self.articleZipf
            params = [ int(self.articleZipf.next()) ]
        # Denormalization
        elif config["experiment"] == 2:
            params = [ random.randint(0, self.num_articles) ]
        # Indexing
        elif config["experiment"] == 3:
            params = [ random.randint(0, self.num_articles) ]
        else:
            raise Exception("Unexpected experiment type %d" % config["experiment"]) 
        
        return (txnName, params)
    ## DEF
        
    def executeImpl(self, config, txn, params):
        assert self.conn != None
        assert "experiment" in config
        
        if self.debug:
            LOG.debug("Executing %s / %s [denormalize=%s]" % (txn, str(params), config["denormalize"]))
        
        # Sharding Key
        if config["experiment"] == 1:
            self.expDenormalization(params[0])
        # Denormalization
        elif config["experiment"] == 2:
            self.expDenormalization(config["denormalize"], params[0])
        # Indexing - Variant 1
        elif config["experiment"] == 5:
            self.expIndexes(config["denormalize"], params[0])
            pass
        # Indexing - Variant 2
        elif config["experiment"] == 6:
            pass
        
        return
    ## DEF
    
    
    def expSharding(self, articleId):
        """
        For this experiment, we will shard articles by their autoinc id and then 
        by their id+timestamp. This will show that sharding on just the id won't
        work because of skew, but by adding the timestamp the documents are spread out
        more evenly. If we shard on the id+timestamp, will queries that only use the 
        timestamp get redirected to a mininal number of nodes?
        Not sure if this is a good experiment to show this. Might be too trivial.
        """
        article = self.db[constants.ARTICLE_COLL].find_one({"id": articleId}, {"comments": 0})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            pass
        return
    ## DEF
    
    def expDenormalization(self, denormalize, articleId):
        """
        In our microbenchmark we should have a collection of articles and collection of 
        article comments. The target workload will be to grab an article and grab the 
        top 10 comments for that article sorted by a user rating. In the first experiment,
        we will store the articles and comments in separate collections.
        In the second experiment, we'll embedded the comments inside of the articles.
        Not sure if we can do that in a single query... 
        What we should see is that the system is really fast when it can use a single 
        query for documents that contain a small number of embedded documents. But 
        then as the size of the comments list for each article increases, the two query
        approach is faster. We may want to also have queries that append to the comments
        list to show that it gets expensive to write to documents with a long list of 
        nested documents
        """
        
        article = self.db[constants.ARTICLE_COLL].find_one({"id": articleId})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            pass
        assert article["id"] == articleId
        if denormalize:
            comments = self.db[constants.COMMENT_COLL].find({"article": articleId})
        else:
            assert "comments" in article, pformat(article)
            comments = article["comments"]
        return
    ## DEF
    
    def experiment3(self, config, channel, msg):
        """
        In our final benchmark, we compared the performance difference between a query on 
        a collection with (1) no index for the query's predicate, (2) an index with only one 
        key from the query's predicate, and (3) a covering index that has all of the keys 
        referenced by that query.
        What do we want to vary here on the x-axis? The number of documents in the collection?
        """
        pass
## CLASS