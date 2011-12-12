#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging
import types
import pymongo
import mongokit
import MySQLdb as mdb
from datetime import datetime
from pprint import pprint,pformat
from ConfigParser import SafeConfigParser

import catalog
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s\n%s" % (constants.PROJECT_NAME, constants.PROJECT_URL))
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--host', type=str, help='MySQL host name')
    aparser.add_argument('--name', type=str, help='MySQL database name')
    aparser.add_argument('--user', type=str, help='MySQL username')
    aparser.add_argument('--pass', type=str, help='MySQL password')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)

    if not args['config']:
        logging.error("Missing configuration file")
        print
        aparser.print_help()
        sys.exit(1)
    logging.debug("Loading configuration file '%s'" % args['config'])
    cparser = SafeConfigParser()
    cparser.read(os.path.realpath(args['config'].name))
    config.setDefaultValues(cparser)
    
    ## ----------------------------------------------
    
    ## Connect to MongoDB
    try:
        hostname = cparser.get(config.SECT_MONGODB, 'hostname')
        port = cparser.getint(config.SECT_MONGODB, 'port')        
        conn = mongokit.Connection(host=hostname, port=port)
    except:
        LOG.error("Failed to connect to MongoDB at %s:%s" % (config['hostname'], config['port']))
        raise
    
    ## Register our objects with MongoKit
    conn.register([ catalog.Collection ])
    schema_db = conn[config['schema_db']]
    
    ## ----------------------------------------------
    
    mysql_conn = mdb.connect(host=args['host'], db=args['name'], user=args['user'], passwd=args['pass'])
    c1 = mysql_conn.cursor()
    c1.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s", args['name'])
    for row in c1:
        tbl_name = row[0]
        coll_catalog = schema_db.Collection()
        coll_catalog['name'] = tbl_name
        coll_catalog['shard_keys'] = [ ]
        coll_catalog['fields'] = { }
        
        c2 = mysql_conn.cursor()
        c2.execute("""
            SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME=%s
        """, (args['name'], tbl_name))
        print tbl_name
        for col_row in c2:
            col_name = col_row[0]
            col_type = catalog.sqlTypeToPython(col_row[1])
            coll_catalog["fields"][col_name] = {
                'type': catalog.fieldTypeToString(col_type),
            }
            #print "  ", col_row
        #print
        
        # TODO: Get the index information from MySQL for this table
        coll_catalog['indexes'] = [ ]
        
        # TODO: Perform some analysis on the table to figure out the information 
        # that we need for selecting the candidates and our cost model
        
        print pformat(coll_catalog)
        coll_catalog.save()
    ## FOR
    
    # TODO: Ingest a MySQL query log and convert it into our workload.Sessions objects
    
## MAIN