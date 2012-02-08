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

import sql2mongo

import workload
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
    schema_db = conn[cparser.get(config.SECT_MONGODB, 'schema_db')]
    schema_db.drop_collection(constants.CATALOG_COLL)
    ## ----------------------------------------------
    
    mysql_conn = mdb.connect(host=args['host'], db=args['name'], user=args['user'], passwd=args['pass'])
    c1 = mysql_conn.cursor()
    c1.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s", args['name'])
    quick_look = {}
    for row in c1:
        tbl_name = row[0]
        coll_catalog = schema_db.Collection()
        coll_catalog['name'] = unicode(tbl_name)
        coll_catalog['shard_keys'] = { }
        coll_catalog['fields'] = { }
        coll_catalog['indexes'] = { }
        quick_look[coll_catalog['name']] = []
        
        c2 = mysql_conn.cursor()
        c2.execute("""
            SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME=%s
        """, (args['name'], tbl_name))
        
        for col_row in c2:
            col_name = col_row[0]
            quick_look[coll_catalog['name']].append(col_name)
            col_type = catalog.sqlTypeToPython(col_row[1])
            coll_catalog["fields"][col_name] = {
                'type': catalog.fieldTypeToString(col_type),
            }
        ## FOR
        
        # TODO: Get the index information from MySQL for this table
        sql = "SHOW INDEXES FROM " + args['name'] + "." + tbl_name
        c3 = mysql_conn.cursor()
        c3.execute(sql)
        index_name = None
        for ind_row in c3:
            if index_name <> ind_row[2]:
                coll_catalog['indexes'][ind_row[2]] = []
                index_name = ind_row[2]
            coll_catalog['indexes'][ind_row[2]].append(ind_row[4])
        ## FOR

        # TODO: Perform some analysis on the table to figure out the information 
        # that we need for selecting the candidates and our cost model

        coll_catalog.validate()
        coll_catalog.save()
    ## FOR

    # TODO: Ingest a MySQL query log and convert it into our workload.Sessions objects
    c4 = mysql_conn.cursor()
    c4.execute("""
        SELECT * FROM general_log ORDER BY thread_id, event_time;	
    """)
    conn.register([workload.Session])
    workload_db = conn[cparser.get(config.SECT_MONGODB, 'workload_db')]
    workload_db.drop_collection(constants.WORKLOAD_SESSIONS)
    thread_id = None
    '''
    [0] = event_time
    [1] = user_host
    [2] = thread_id
    [3] = server_id
    [4] = command_type
    [5] = argument
    '''
    for row in c4:
        if row[2] <> thread_id :
            thread_id = row[2]
        ## ENDIF
        if row[5] <> '' :
            mongo = sql2mongo.Sql2mongo(row[5], quick_look)
            if mongo.query_type <> 'UNKNOWN' :
                session = workload_db.Session()
                session['ip1'] = u'test-ip1'
                session['ip2'] = u'test-ip2'
                session['uid'] = int(thread_id)
                session.save()
                #print row[5]
                #print mongo.render()
                pass
            ## ENDIF
        ## ENDIF
    ## ENDFOR
## MAIN