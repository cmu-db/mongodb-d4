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
import mongokit
from pprint import pprint, pformat

# Designer
import workload

# Benchmark
import constants
from util import *
from api.abstractworker import AbstractWorker
from api.message import *

LOG = logging.getLogger(__name__)

DB_NAME = 'replay'

class ReplayWorker(AbstractWorker):
    
    def initImpl(self, config, channel):
        # We need two connections. One to the target database and one
        # to the workload database. 
        
        ## ----------------------------------------------
        ## WORKLOAD REPLAY CONNECTION
        ## ----------------------------------------------
        self.replayConn = None
        try:
            self.replayConn = mongokit.Connection(host=config['replayhost'], port=config['replayport'])
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (config['replayhost'], config['replayport']))
            raise
        assert self.replayConn
        self.replayConn.register([ workload.Session ])
        self.replayCurosr = self.replayConn
        
        ## ----------------------------------------------
        ## TARGET CONNECTION
        ## ----------------------------------------------
        self.conn = None
        try:
            self.conn = pymongo.Connection(config['host'], config['port'])
        except:
            LOG.error("Failed to connect to MongoDB at %s:%s" % (config['host'], config['port']))
            raise
        assert self.conn
        self.db = self.conn[constants.DB_NAME]
        
        # TODO: Figure out we're going to load the database. I guess it
        # would come from the workload database, right?
        if config["reset"]:
            LOG.info("Resetting database '%s'" % constants.DB_NAME)
            self.conn.drop_database(constants.DB_NAME)
        
        # TODO: We need to also load in the JSON design file generated
        # by the designer tool.
       

        return  
    ## DEF
    
    def loadImpl(self, config, channel, msg):
        assert self.conn != None
        
        # TODO: Get the original database from the replayConn and then
        # massage it according to the design
        
        # TODO: Go through the sample workload and rewrite the queries according
        # to the design. I think that this just means that we need to identify whether
        # we have any denormalized collections.

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
        
        # The first parameter is going to be the Session that we need to replay
        # The txn doesn't matter
        sess = params[0]
        
        for op in sess['operations']:
            # TODO: We should check whether they are trying to make a query
            # against a collection that we don't know about so that we can print
            # a nice error message. This is because we know that we should never
            # have an operation that doesn't touch a collection that we don't already
            # know about.
            coll = op['collection']
            
            # QUERY
            if op['type'] == "$query":
                # The query field has our where clause
                whereClause = op['content']['query']
                
                # And the second element is the projection
                fieldsClause = { }
                if 'fields' in op['content']:
                    fieldsClause = op['content']['fields']

                # Execute!
                # I don't think there is anything we need to do with the result
                # TODO: Need to check the performance difference of find vs find_one
                # TODO: We need to handle counts + sorts + limits
                result = self.db[coll].find(whereClause, fieldsClause)
                
            # UPDATE
            elif op['type'] == "$update":
                # The first element in the content field is the WHERE clause
                whereClause = op['content'][0]
                
                # The second element has what we're trying to update
                updateClause = op['content'][1]
                
                # Let 'er rip!
                # TODO: Need to handle 'upsert' and 'multi' flags
                # TODO: What about the 'manipulate' or 'safe' flags?
                result = self.db[coll].update(whereClause, updateClause)
                
            # INSERT
            elif op['type'] == "$insert":
                # Just get the payload and fire it off
                # There's nothing else to really do here
                result = self.db[coll].insert(op['content'])
                
            # DELETE
            elif op['type'] == "$delete":
                # The first element in the content field is the WHERE clause
                whereClause = op['content'][0]
                
                # SAFETY CHECK: Don't let them accidently delete the entire collection
                assert whereClause and len(whereClause) > 0
                
                # I'll see you in hell!!
                result = self.db[coll].remove(whereClause)
                
        ## FOR
        
        
        
        if self.debug:
            LOG.debug("Executing %s / %s [denormalize=%s]" % (txn, str(params), config["denormalize"]))
        
        # Sharding Key
        if config["experiment"] == 1:
            self.expSharding(params[0])
        # Denormalization
        elif config["experiment"] == 2:
            self.expDenormalization(config["denormalize"], params[0])
        # Indexing
        elif config["experiment"] == 3:
            self.expIndexes(params[0])
        # Busted!
        else:
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
    
    def expIndexes(self, articleId):
        """
        In our final benchmark, we compared the performance difference between a query on 
        a collection with (1) no index for the query's predicate, (2) an index with only one 
        key from the query's predicate, and (3) a covering index that has all of the keys 
        referenced by that query.
        What do we want to vary here on the x-axis? The number of documents in the collection?
        """
        
        article = self.db[constants.ARTICLE_COLL].find({"id": articleId}, {"id", "date", "author"})
        if not article:
            LOG.warn("Failed to find %s with id #%d" % (constants.ARTICLE_COLL, articleId))
        
        return
## CLASS