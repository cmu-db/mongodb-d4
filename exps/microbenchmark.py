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

from datetime import datetime, timedelta
from pprint import pprint

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)





    

    
    
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