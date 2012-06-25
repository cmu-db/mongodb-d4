#!/usr/bin/env python
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
import argparse
import logging
from pymongo import Connection

# mongodb-d4
sys.path.append("../workload")
sys.path.append("../util")
import parser
from util.histogram import Histogram

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

### DEFAULT VALUES
WORKLOAD_DB = "metadata"
WORKLOAD_COLLECTION = "workload01"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

# global vars
workload_db = None
workload_col = None

def initDB(hostname, port, w_db, w_col):
    global workload_db
    global workload_col
    LOG.info("Connecting to MongoDB at %s:%d" % (hostname, port))
    
    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    workload_db = connection[w_db]
    workload_col = w_col

    return connection


def print_stats(host, port, w_db, w_col):
    print ""
    LOG.info("..:: MongoDesigner Workload Info ::..")
    print ""

    #start connection and set global variables...
    connection = initDB(host, port, w_db, w_col)
    
    LOG.info("="*50)
    
    session_cnt = workload_db[workload_col].find().count()
    LOG.info("Number of sessions: %d", session_cnt)
    LOG.info("Number of operations per session:")
    
    maxOpCnt = 0
    minOpCnt = sys.maxint
    vals = []
    
    typeCnts = Histogram()
    for session in workload_db[workload_col].find():
        for op in session['operations']:
            typeCnts.put(op['type'])
            
        op_cnt = len(session['operations'])
        minOpCnt = min(op_cnt, minOpCnt)
        maxOpCnt = max(op_cnt, maxOpCnt)
        vals.append(op_cnt)
    ## FOR
    avgOpCnt = None
    if vals:
        avgOpCnt = "%.2f" % float(sum(vals)) / float(len(vals))
        
    LOG.info("%10s: %d" % ("min", minOpCnt))
    LOG.info("%10s: %d" % ("max", maxOpCnt))
    LOG.info("%10s: %s" % ("avg", avgOpCnt))
    
    LOG.info("Number of operations by type:")
    for opType in typeCnts.values():
        LOG.info("%10s: %d" % (opType, typeCnts[opType]))
    ## FOR
    
    LOG.info("="*50)
    
    return


def main():
    global headerRegex

    aparser = argparse.ArgumentParser(description='MongoDesigner Workload Info')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--workload_db', help='the database where you want to store the traces', default=WORKLOAD_DB)
    aparser.add_argument('--workload_col', help='the collection where you want to store the traces', default=WORKLOAD_COLLECTION)
    args = vars(aparser.parse_args())

    
    print_stats(args['host'], args['port'], args['workload_db'], args['workload_col'])

if __name__ == '__main__':
        main()



    
