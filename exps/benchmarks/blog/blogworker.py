# -*- coding: iso-8859-1 -*-
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

import sys
import os
import string
import re
import logging
import traceback
import pymongo
from datetime import datetime
from pprint import pprint, pformat
import time
import constants
from util import *
from api.abstractworker import AbstractWorker
from api.message import *

LOG = logging.getLogger(__name__)

# BLOGWORKER
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
# 
# This is the worker for the 'blog' microbenchmark in the paper
# There are three types of experiments that we want to perform on the 
# data generated by this code. These experiments are designed to higlight
# different aspects of database design in MongoDB by demonstrated the
# performance trade-offs of getting it wrong.
# For each experiment type, there are two variations of the workload. The first 
# of which is the "correct" design choice and the second is the "bad" design
# choice. Yes, this is somewhat a simplistic view, but it's mostly 
# meant to be an demonstration rather than a deep analysis of the issues:
#
# Experiment #1: SHARDING KEYS
# For this experiment, we will shard articles by their autoinc id and then 
# by their id+timestamp. This will show that sharding on just the id won't
# work because of skew, but by adding the timestamp the documents are spread out
# more evenly.
# 
# Experiment #2: DENORMALIZATION
# In our microbenchmark we should have a collection of articles and collection of 
# article comments. The target workload will be to grab an article and grab the 
# top 10 comments for that article sorted by a user rating. In the first experiment,
# we will store the articles and comments in separate collections.
# In the second experiment, we'll embedded the comments inside of the articles.
# 
# Experiment #3: INDEXES
# In our final benchmark, we compared the performance difference between a query on 
# a collection with (1) no index for the query's predicate, (2) an index with only one 
# key from the query's predicate, and (3) a covering index that has all of the keys 
# referenced by that query.
# 
class BlogWorker(AbstractWorker):
  
    
    
    def initImpl(self, config, msg):
        self.articleCounterDocumentId = None
        # A list of booleans that we will randomly select
        # from to tell us whether our op should be a read or write
        self.workloadWrite = [ ]
        for i in xrange(0, constants.WORKLOAD_READ_PERCENT):
            self.workloadWrite.append(False)
        for i in xrange(0, constants.WORKLOAD_WRITE_PERCENT):
            self.workloadWrite.append(True)
        
        # Total number of articles in database
        #the number of articles (e.g 10000) or 10000 x scalefactor is not the real number, because if we have 16 workers, 
        #the number of articles is not equaly divisible by the number of workers, and will be lower than that.
        self.clientprocs = int(self.config["default"]["clientprocs"])
        self.num_articles = int(int(self.getScaleFactor() * constants.NUM_ARTICLES) / self.clientprocs) * self.clientprocs
        self.firstArticle = msg[0]
        self.lastArticle = msg[1]
        self.lastCommentId = None
        self.config[self.name]["commentsperarticle"]
        self.articleZipf = ZipfGenerator(self.num_articles, float(config[self.name]["skew"]))
        LOG.info("Worker #%d Articles: [%d, %d]" % (self.getWorkerId(), self.firstArticle, self.lastArticle))
        numComments = int(config[self.name]["commentsperarticle"])
        
        # Zipfian distribution on the number of comments & their ratings
        self.commentsZipf = ZipfGenerator(numComments,float(config[self.name]["skew"]))
        self.ratingZipf = ZipfGenerator(constants.MAX_COMMENT_RATING+1, float(config[self.name]["skew"]))
        self.db = self.conn[config['default']["dbname"]]   
        
        #precalcualtiong the authors names list to use Zipfian against them
        self.authors = [ ]
        for i in xrange(0, constants.NUM_AUTHORS):
            #authorSize = constants.AUTHOR_NAME_SIZE
            if config[self.name]["experiment"] == constants.EXP_INDEXING:
                self.authors.append("authorname%0128d" % i)
            else:
                self.authors.append("authorname"+str(i))
        self.authorZipf = ZipfGenerator(constants.NUM_AUTHORS,float(config[self.name]["skew"]))
        
        #precalculating tags
        self.tags = [ ]
        for i in xrange(0, constants.NUM_TAGS):
            #authorSize = constants.AUTHOR_NAME_SIZE
            if config[self.name]["experiment"] == constants.EXP_INDEXING:
                self.tags.append("tag%0128d" % i)
            else:    
                self.tags.append("tag"+str(i))
        self.tagZipf = ZipfGenerator(constants.NUM_TAGS,float(config[self.name]["skew"]))
        
        #precalcualtiong the dates list to use Zipfian against them
        self.dates = [ ]
        #dates in reverse order as we want to have the most recent to be more "accessed" by Zipfian
        self.datecount=0
        epochToStartInSeconds = int(time.mktime(constants.START_DATE.timetuple()))
        epochToStopInSeconds = int(time.mktime(constants.STOP_DATE.timetuple()))
        # 1day = 24*60*60sec = 86400
        for i in xrange(epochToStopInSeconds,epochToStartInSeconds,-86400):
            self.dates.append(datetime.fromtimestamp(i))
            self.datecount +=1
        self.dateZipf = ZipfGenerator(self.datecount,float(config[self.name]["skew"]))
        
        
        
        if self.getWorkerId() == 0:
            if config['default']["reset"]:
                LOG.info("Resetting database '%s'" % config['default']["dbname"])
                self.conn.drop_database(config['default']["dbname"])
            
            ## SHARDING
            if config[self.name]["experiment"] == constants.EXP_SHARDING:
                self.enableSharding(config)
        ## IF
        
        ## The next operation that we need to execute	
        ## If it's empty, then just execute whatever it is that we're suppose to
        self.nextOp = [ ]
        
        
        
        #self.initNextCommentId(config[self.name]["maxCommentId"])
    ## DEF
    
    def enableSharding(self, config):
        assert self.db != None
        
        # Enable sharding on the entire database
        try:
            result = self.db.command({"enablesharding": self.db.name})
            assert result["ok"] == 1, "DB Result: %s" % pformat(result)
        except:
            LOG.error("Failed to enable sharding on database '%s'" % self.db.name)
            raise
        
        # Generate sharding key patterns
        # CollectionName -> Pattern
        # http://www.mongodb.org/display/DOCS/Configuring+Sharding#ConfiguringSharding-ShardingaCollection
        shardingPatterns = { }
        
        if config[self.name]["sharding"] == constants.SHARDEXP_SINGLE:
            shardingPattern = {articles : { id : 1}}
        
        elif config[self.name]["sharding"] == constants.SHARDEXP_COMPOUND:
            shardingPattern = {articles : {id : 1, hashid : 1}}
        
        else:
            raise Exception("Unexpected sharding configuration type '%d'" % config["sharding"])
        
        # Then enable sharding on each of these collections
        for col,pattern in shardingPatterns.iteritems():
            LOG.debug("Sharding Collection %s.%s: %s" % (self.db.name, col, pattern))
            try:
                result = self.db.command({"shardcollection": col, "key": pattern})
                assert result["ok"] == 1, "DB Result: %s" % pformat(result)
            except:
                LOG.error("Failed to enable sharding on collection '%s.%s'" % (self.db.name, col))
                raise
        ## FOR
        
        LOG.debug("Successfully enabled sharding on %d collections in database %s" % \
                  (len(Patterns, self.db.name)))
    ## DEF
 
    ## ---------------------------------------------------------------------------
    ## STATUS
    ## ---------------------------------------------------------------------------
    
    def statusImpl(self, config, channel, msg):
        result = { }
        for col in self.db.collection_names():
            stats = self.db.validate_collection(col)
            result[self.db.name + "." + col] = (stats.datasize, stats.nrecords)
        ## FOR
        return (result)
    ## DEF
 
    ## ---------------------------------------------------------------------------
    ## LOAD
    ## ---------------------------------------------------------------------------
    
    def loadImpl(self, config, channel, msg):
        assert self.conn != None
        
        # HACK: Setup the indexes if we're the first client
        if self.getWorkerId() == 0:
            self.db[constants.ARTICLE_COLL].drop_indexes()
            self.db[constants.COMMENT_COLL].drop_indexes()
            
            
            ## INDEXES CONFIGURATION
            
            
            if config[self.name]["experiment"] == constants.EXP_INDEXING:
                #article(id)
                #LOG.info("Creating index %s(id)" % self.db[constants.ARTICLE_COLL].full_name)
                #self.db[constants.ARTICLE_COLL].ensure_index([("id", pymongo.ASCENDING)])
                #article(author)       
                #LOG.info("Creating index %s(author)" % self.db[constants.ARTICLE_COLL].full_name) 
                #self.db[constants.ARTICLE_COLL].ensure_index([("author", pymongo.ASCENDING)])
                
                trial = int(config[self.name]["indexes"])
                
                if trial == 0:
                    #article(id)
                    LOG.info("Creating index %s(id)" % self.db[constants.ARTICLE_COLL].full_name) 
                    self.db[constants.ARTICLE_COLL].ensure_index([("id", pymongo.ASCENDING)])
                    
                elif trial == 1:
                    #article(hashid)
                    LOG.info("Creating index %s(hashid)" % self.db[constants.ARTICLE_COLL].full_name) 
                    self.db[constants.ARTICLE_COLL].ensure_index([("hashid", pymongo.ASCENDING)])
            
            elif config[self.name]["experiment"] == constants.EXP_DENORMALIZATION:
                LOG.info("Creating primary key indexes for %s" % self.db[constants.ARTICLE_COLL].full_name) 
                self.db[constants.ARTICLE_COLL].ensure_index([("id", pymongo.ASCENDING)])
                
                if not config[self.name]["denormalize"]:
                    LOG.info("Creating indexes on Comment (articleId) %s" % self.db[constants.COMMENT_COLL].full_name)
                    self.db[constants.COMMENT_COLL].ensure_index([("article", pymongo.ASCENDING)])
                    LOG.info("Creating primary indexes on Comment (id) %s" % self.db[constants.COMMENT_COLL].full_name)
                    self.db[constants.COMMENT_COLL].ensure_index([("id", pymongo.ASCENDING)])
                    
            elif config[self.name]["experiment"] == constants.EXPS_SHARDING:
                #NOTE: we don't need an index on articleId only as we have this composite index -> (articleId,articleHashId)
                LOG.info("Creating indexes (hashid) %s" % self.db[constants.ARTICLE_COLL].full_name)
                self.db[constants.ARTICLE_COLL].ensure_index([("hashid", pymongo.ASCENDING)])
                
            
            else:
                raise Exception("Unexpected experiment type %s" % config[self.name]["experiment"])
                   
        ## IF
        
        ## ----------------------------------------------
        ## LOAD ARTICLES
        ## ----------------------------------------------
        articleCtr = 0
        articleTotal = self.lastArticle - self.firstArticle
        commentCtr = 0
        commentTotal= 0
        numComments = int(config[self.name]["commentsperarticle"])
        for articleId in xrange(self.firstArticle, self.lastArticle+1):
            #titleSize = constants.ARTICLE_TITLE_SIZE
            #title = randomString(titleSize)
            #contentSize = constants.ARTICLE_CONTENT_SIZE
            #content = randomString(contentSize)
            #articleTags = []
            #for ii in xrange(0,constants.NUM_TAGS_PER_ARTICLE):
            #     articleTags.append(random.choice(self.tags))
            # 
            #articleDate = randomDate(constants.START_DATE, constants.STOP_DATE)
            #articleIdHash = hash(str(articleId))
            #article = {
            #    "id": long(articleId),
            #    "title": title,
            #    "date": articleDate,
            #    "author": random.choice(self.authors),
            #    "hashid" : articleHashId,
            #    "content": content,
            #    "numComments": numComments,
            #    "tags": articleTags,
            #    "views": 0,
            #}
            #articleCtr+=1;
            #if config[self.name]["denormalize"]:
            #    article["comments"] = [ ]
            #self.db[constants.ARTICLE_COLL].insert(article)
            self.insertNewArticle(config)
            articleCtr+=1
            ## ----------------------------------------------
            ## LOAD COMMENTS
            ## ----------------------------------------------
            commentsBatch = [ ]
            LOG.debug("Comments for article %d: %d" % (articleId, numComments))
            for ii in xrange(0, numComments):
                #lastDate = randomDate(articleDate, constants.STOP_DATE)
                commentAuthor = random.choice(self.authors)
                commentContent = randomString(constants.COMMENT_CONTENT_SIZE)
                
                comment = {
                    "id": str(articleId)+"|"+str(ii),
                    "article": long(articleId),
                    "date": randomDate(articleDate, constants.STOP_DATE), 
                    "author": commentAuthor,
                    "comment": commentContent,
                    "rating": int(self.ratingZipf.next()),
                    "votes": 0,
                } # Check whether we have a next op that we need to execute
                commentCtr += 1
                commentsBatch.append(comment) 
                #if config[self.name]["denormalize"]:
                    #self.db[constants.ARTICLE_COLL].update({"id": articleId},{"$push":{"comments":comment}}) 
                    
                if not config[self.name]["denormalize"]:
                    self.db[constants.COMMENT_COLL].insert(comment) 
            ## FOR (comments)
            if config[self.name]["denormalize"]:
                self.db[constants.ARTICLE_COLL].update({"id": articleId},{"$pushAll":{"comments":commentsBatch}})  
        ## FOR (articles)
        
        if config[self.name]["denormalize"]:
            if articleCtr % 100 == 0 or articleCtr % 100 == 1 :
                self.loadStatusUpdate(articleCtr / articleTotal)
                LOG.info("ARTICLE: %6d / %d" % (articleCtr, articleTotal))

        LOG.info("ARTICLES PER THREAD: %6d / %d" % (articleCtr, articleCtr))
        LOG.info("COMMENTS PER THREAD: %6d / %d" % (commentCtr,commentCtr))        
        LOG.info("TOTAL ARTICLES: %6d / %d" % (self.clientprocs*articleCtr, self.clientprocs*articleCtr))
        LOG.info("TOTAL COMMENTS: %6d / %d" % (self.clientprocs*commentCtr,self.clientprocs*commentCtr))   
    ## DEF
    
    ## ---------------------------------------------------------------------------
    ## EXECUTION INITIALIZATION
    ## ---------------------------------------------------------------------------
    
    def executeInitImpl(self, config):
        pass
    ## DEF
    
    ## ---------------------------------------------------------------------------
    ## WORKLOAD EXECUTION
    ## ---------------------------------------------------------------------------
    
    def next(self, config):
        assert "experiment" in config[self.name]
        
        # Check whether we have a next op that we need to execute
        if len(self.nextOp) > 0:
            return self.nextOp.pop(0)
            
        # Otherwise just figure out what the next random thing
        # it is that we need to do...
 
        if config[self.name]["experiment"] == constants.EXP_DENORMALIZATION: 
            articleId = self.articleZipf.next()
            opName = "readArticleTopCommentsIncCommentVotes"
            return (opName, (articleId,))
            
        elif config[self.name]["experiment"] == constants.EXP_SHARDING:
            trial = int(config[self.name]["sharding"])
            if trial == 0:
                #single sharding key
                articleId = self.articleZipf.next()
                opName = "readArticleById"
                return (opName, (articleId,))
            elif trial == 1:
                #composite sharding key
                articleId = self.articleZipf.next()
                articleHashId = hash(str(articleId))
                opName = "readArticleAndHashId"
                return (opName, (articleId,articleIdHash))
               
        elif config[self.name]["experiment"] == constants.EXP_INDEXING:              
            trial = int(config[self.name]["sharding"])
            if trial == 0:
                articleId = self.articleZipf.next()
                opName = "readArticleById"
            elif trial ==1:
                articleId = self.articleZipf.next()
                articleHashId = hash(str(articleId))
                opName = "readArticleByHashId"
            return (opName, (articleIdHash))
   ## DEF
        
    def executeImpl(self, config, op, params):
        #global opCount;
        assert self.conn != None
        assert "experiment" in config[self.name]
        result = 0
        if self.debug:
            LOG.debug("Executing %s / %s" % (op, str(params)))
        
        m = getattr(self, op)
        assert m != None, "Invalid operation name '%s'" % op
        try:
            result = m(config, *params)
            #result = m(*params)
        except:
            LOG.warn("Unexpected error when executing %s" % op)
            raise
        
        return result # number of operations
    ## DEF
    
    def readArticleById(self,config, articleId):
        article = self.db[constants.ARTICLE_COLL].find_one({"id": articleId})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            return
        assert article["id"] == articleId, \
            "Unexpected invalid %s record for id #%d" % (constants.ARTICLE_COLL, articleId)
        return 1
    #DEF
    
    def readArticlesByTag(self,config, tag):
        articles = self.db[constants.ARTICLE_COLL].find({"tags": tag})
        for article in articles:
            pass
        return 1
    #DEF
    
    def readArticlesByAuthor(self,config,author):
        articles = self.db[constants.ARTICLE_COLL].find({"author": author})
        for article in articles:
            pass 
        return 1
    #DEF
    
    def readArticlesByDate(self,config,date):
        article = self.db[constants.ARTICLE_COLL].find({"date": date})
        for article in articles:
            pass
        return 1
    #DEF
    
    def readArticleByHashId(self,config,hashid):
        article = self.db[constants.ARTICLE_COLL].find_one({"hashid": hashid})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            return
        assert article["hashid"] == hashid, \
            "Unexpected invalid %s record for id #%d" % (constants.ARTICLE_COLL, articleId)
        return 1
    #DEF
    
    def readArticleByIdAndHashId(self,config,articleId,hashid):
        article = self.db[constants.ARTICLE_COLL].find_one({"id":articleId,"hashid": hashid})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            return
        assert article["hashid"] == hashid, \
            "Unexpected invalid %s record for id #%d" % (constants.ARTICLE_COLL, articleId)
        assert article["id"] == articleId, \
            "Unexpected invalid %s record for id #%d" % (constants.ARTICLE_COLL, articleId)   
        return 1
    #DEF
    
    def readArticlesByAuthorAndDate(self,config,author,date):
        articles = self.db[constants.ARTICLE_COLL].find({"author":author,"date": date})
        for article in articles:
            pass 
        return 1
    #DEF
    
    def readArticlesByAuthorAndTag(self,config,author,tag):
        articles = self.db[constants.ARTICLE_COLL].find({"author":author,"tags": tag})
        for article in articles:
            pass
        return 1
    #DEF
    
    ## ---------------------------------------------------------------------------
    ## DENORMALIZATION
    ## ---------------------------------------------------------------------------
    
    
    def updateArticleComments(self, config, articleId):
        commentsPerArticle = int(config[self.name]["commentsperarticle"])-1
        
        if not config[self.name]["denormalize"]:
            commentId = "%s|%d" % (articleId, commentsPerArticle)
            self.db[constants.COMMENT_COLL].update({"id": commentId}, {"$inc" : {"votes":1}}, False)
        else:
            commentId = "comments.%d.votes" % random.randint(0, commentsPerArticle)
            self.db[constants.ARTICLE_COLL].update({"id": articleId}, {'$inc': {commentId: 1}}, False)
            
        return 1
    ## DEF
    
    def readArticleTopCommentsIncCommentVotes(self,config,articleId):
        """We are searching for the comments that had been written for the article with articleId"""
        
        opCount = 0
        if not config[self.name]["denormalize"]:
            article = self.db[constants.ARTICLE_COLL].find_one({"id": articleId})
            comments = self.db[constants.COMMENT_COLL].find({"article": articleId}).limit(100)
            opCount = 2
            for comment in comments:
                pass

        else:
            article = self.db[constants.ARTICLE_COLL].find_one({"id": articleId})
            opCount = 1

        if article is None:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
            return opCount
        assert article["id"] == articleId, \
            "Unexpected invalid %s record for id #%d" % (constants.ARTICLE_COLL, articleId) 
            
        # With probability 20% we update a field in a random comment of this articleid
        if random.random() >= 0.8:
            # Queue up the write!
            self.nextOp.append(("updateArticleComments", (articleId,)))
                
        return opCount    
    
    
    def insertNewArticle(self,config):
        articleId = self.findAndIncreaseArticleCounter()
        titleSize = constants.ARTICLE_TITLE_SIZE
        title = randomString(titleSize)
        contentSize = constants.ARTICLE_CONTENT_SIZE
        content = randomString(contentSize)
        numComments = int(config[self.name]["commentsperarticle"])
        articleTags = []
        for ii in xrange(0,constants.NUM_TAGS_PER_ARTICLE):
            articleTags.append(random.choice(self.tags))
        articleDate = randomDate(constants.START_DATE, constants.STOP_DATE)
        articleHashId = hash(str(articleId))
        article = {
            "id": long(articleId),
            "title": title,
            "date": articleDate,
            "author": random.choice(self.authors),
            "hashid" : articleHashId,
            "content": content,
            "numComments": numComments,
            "tags": articleTags,
            "views": 0,
        }
        if config[self.name]["denormalize"]:
            article["comments"] = [ ]
        self.db[constants.ARTICLE_COLL].insert(article)
        return 1
    #DEF

    
    
    
    def getArticleCounterQuery(self):
        articleCounter = self.db[constants.ARTICLE_COLL].find_one({"id": -9999999})
        if self.articleCounterDocumentId is None or articleCounter is None: 
           articleCounter = { "id" : -9999999, "nextArticleId": -1}
           self.db[constants.ARTICLE_COLL].insert(articleCounter)
           articleCounter = self.db[constants.ARTICLE_COLL].find_one({"id": -9999999})
           self.articleCounterDocumentId = articleCounter[u'_id']
           LOG.debug("firsttime"+str(self.articleCounterDocumentId))
        articleCounterQuery = {'_id':self.articleCounterDocumentId}
        return articleCounterQuery
    #DEF      
    
    def findAndIncreaseArticleCounter(self):    
        query = self.getArticleCounterQuery()
        update = {'$inc': {"nextArticleId": 1}}
        counter = self.db[constants.ARTICLE_COLL].find_and_modify(query,update,False)
        return long(counter[u'nextArticleId'])
    
## CLASS
