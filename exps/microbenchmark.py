#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging
import string
import math
import bisect
import random
import pymongo
from datetime import datetime, timedelta
from pprint import pprint

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)




def string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return (start + timedelta(seconds=random_second))
## DEF
    
## -----------------------------------------------------
## Zipfian Distribution Generator
## -----------------------------------------------------
class ZipfGenerator: 
    
    def __init__(self, n, alpha): 
        # Calculate Zeta values from 1 to n: 
        tmp = [1. / (math.pow(float(i), alpha)) for i in range(1, n+1)] 
        zeta = reduce(lambda sums, x: sums + [sums[-1] + x], tmp, [0]) 
        
        # Store the translation map: 
        self.distMap = [x / zeta[-1] for x in zeta] 
    
    def next(self): 
        # Take a uniform 0-1 pseudo-random value: 
        u = random.random()  

        # Translate the Zipf variable: 
        return bisect.bisect(self.distMap, u) - 1

        return i - 1 
## CLASS
    
    
def show_results(start, stop, num) :
    print num, 'queries executed'
    elapsed = stop - start
    print elapsed, 'time elapsed'
    avg = elapsed / num
    print avg, 'time per query'
    return None

    
    
## -----------------------------------------------------
## Generate Synthetic Data for Micro-Benchmarking
## -----------------------------------------------------
def generateData(conn, num_articles, denormalize = False):
    logging.info("Initializing database '%s'" % DB_NAME)
    conn.drop_database(DB_NAME)
    
    ## Precompute our blog article authors
    authors = [ ]
    for i in xrange(0, NUM_AUTHORS):
        authorSize = int(random.gauss(MAX_AUTHOR_SIZE/2, MAX_AUTHOR_SIZE/4))
        authors.append(string_generator(authorSize))
    ## FOR
    
    ## Zipfian distribution on the number of comments & their ratings
    commentsZipf = ZipfGenerator(MAX_NUM_COMMENTS, 1.0)
    ratingZipf = ZipfGenerator(MAX_COMMENT_RATING, 1.0)
    
    logging.info('Begin generating synthetic data')
    i = 0
    commentId = 0
    for i in xrange(0, num_articles):
        titleSize = int(random.gauss(MAX_TITLE_SIZE/2, MAX_TITLE_SIZE/4))
        contentSize = int(random.gauss(MAX_CONTENT_SIZE/2, MAX_CONTENT_SIZE/4))
        
        title = string_generator(titleSize)
        slug = list(title)
        for idx in xrange(0, titleSize):
            if random.randint(0, 10) == 0:
                slug[idx] = "-"
        ## FOR
        slug = "".join(slug)
        articleDate = random_date(START_DATE, STOP_DATE)
        
        article = {
            "id": i,
            "title": title,
            "date": articleDate,
            "author": random.choice(authors),
            "slug": slug,
            "content": string_generator(contentSize)
        }

        numComments = commentsZipf.next()
        lastDate = articleDate
        for ii in xrange(0, numComments):
            lastDate = random_date(lastDate, STOP_DATE)
            comment = {
                "id": commentId,
                "article": i,
                "date": lastDate, 
                "author": string_generator(int(random.gauss(MAX_AUTHOR_SIZE/2, MAX_AUTHOR_SIZE/4))),
                "comment": string_generator(int(random.gauss(MAX_COMMENT_SIZE/2, MAX_COMMENT_SIZE/4))),
                "rating": int(ratingZipf.next())
            }
            if denormalize == False:
                conn[DB_NAME][COMMENT_COLL].insert(comment)
            else:
                if not "comments" in article:
                    article["comments"] = [ ]
                article["comments"].append(comment)
            commentId += 1
        ## FOR (comments)
        
        conn[DB_NAME][ARTICLE_COLL].insert(article)
        
        if i % 1000 == 0 :
            logging.info("ARTICLE: %6d / %d" % (i, num_articles))
    ## FOR (articles)
    
    logging.info("# of ARTICLES: %d" % i)
    logging.info("# of COMMENTS: %d" % commentId)
## DEF

## -----------------------------------------------------
## EXP#1 - Sharding
## -----------------------------------------------------
def generateData(conn, num_articles, duration, denormalize = False):
    articleZipf = ZipfGenerator(num_articles, 1.0)
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    
    aparser = argparse.ArgumentParser(description="Microbenchmark")
    aparser.add_argument('--articles', type=int, default=10000)
    aparser.add_argument('--load', action='store_true')
    aparser.add_argument('--denormalize', action='store_true')
    
    aparser.add_argument('--queries', type=int, default=1000)
    aparser.add_argument('--host', type=str, default="localhost", help='MongoDB hostname')
    aparser.add_argument('--port', type=int, default=27017, help='MongoDB port #')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    
    ## ----------------------------------------------
    ## Connect to MongoDB
    ## ----------------------------------------------
    try:
        conn = pymongo.Connection(args['host'], args['port'])
    except:
        logging.error("Failed to connect to MongoDB at %s:%s" % (args['host'], args['port']))
        raise

    ## -----------------------------------------------------
    ## Load in sample data
    ## -----------------------------------------------------
    if args['load']:
        logging.info('Generating sample data')
        generateData(conn, args['articles'], args['denormalize'])
        
        
    #columns = ['key1', 'key2']
    #limit = args['queries']
    #data_col = 'data'
    #value_col = 'values'
    
    ### -----------------------------------------------------
    ### Read in set of values from which to query
    ### -----------------------------------------------------
    #values = generate_db[value_col].find()
    #key1_values = []
    #key2_values = []
    #for row in values :
        #key1_values.append(row['key1'])
        #key2_values.append(row['key2'])
    #num_values = len(key1_values)
        
    ### -----------------------------------------------------
    ### Execute Micro-Benchmarks for MongoDB Indexes
    ### -----------------------------------------------------
    #print 'Micro-Benchmarking MongoDB Indexes'
    #generate_db[data_col].drop_indexes()
    #print 'Executing queries with no indexes'
    
    #start = time.time()
    #for i in range(limit) :
        #key = random.randint(0, num_values - 1)
        #generate_db[data_col].find({'key1': key1_values[key]})
        #generate_db[data_col].find({'key2': key2_values[key]})
    #end = time.time()
    #show_results(start, end, limit)
    
    
    #generate_db[data_col].create_index('key1')
    #generate_db[data_col].create_index('key2')
    
    #print 'Executing benchmarks on covering indexes'
    #start = time.time()
    #for i in range(limit) :
        #key = random.randint(0, num_values - 1)
        #generate_db[data_col].find({'key1': key1_values[key]})
        #generate_db[data_col].find({'key2': key2_values[key]})
    #end = time.time()
    #show_results(start, end, limit)
## END MAIN