#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import division
from __future__ import with_statement

import os
from pprint import pformat
import sys
import argparse
import logging
import time
import csv
from ConfigParser import RawConfigParser

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# mongodb-d4
import catalog
import workload
from search import Designer
from util import configutil
from util import constants
from util import termcolor

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

LOG = logging.getLogger(__name__)

SCHEMA_COLUMNS = [
    "collection",
    "field",
    "type",
    "cardinality",
    "selectivity",
    "query_use_count",
]

def dumpSchema(collection, fields, writer, spacer=""):
    cur_spacer = spacer
    if len(spacer) > 0: cur_spacer += " - "
    for f_name in sorted(fields.iterkeys(), key=lambda x: x != "_id"):
        row = [ ]
        f = fields[f_name]
        for key in SCHEMA_COLUMNS:
            if key == SCHEMA_COLUMNS[0]:
                val = collection
            elif key == SCHEMA_COLUMNS[1]:
                val = cur_spacer + f_name
            else:
                val = f.get(key, "")
            row.append(val)
        writer.writerow(row)
        
        if len(f.get("fields", [])) > 0:
            dumpSchema(collection, f["fields"], writer, spacer+"  ")
    ## FOR
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s - Distributed Document Database Designer" % constants.PROJECT_NAME)
                                      
    # Configuration File Options
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages.')
    args = vars(aparser.parse_args())

    if args['debug']: LOG.setLevel(logging.DEBUG)
    
    if not args['config']:
        LOG.error("Missing configuration file")
        print
        aparser.print_usage()
        sys.exit(1)
    LOG.debug("Loading configuration file '%s'" % args['config'])
    config = RawConfigParser()
    configutil.setDefaultValues(config)
    config.read(os.path.realpath(args['config'].name))
    
    ## ----------------------------------------------
    ## Connect to MongoDB
    ## ----------------------------------------------
    hostname = config.get(configutil.SECT_MONGODB, 'host')
    port = config.getint(configutil.SECT_MONGODB, 'port')
    assert hostname
    assert port
    try:
        conn = mongokit.Connection(host=hostname, port=port)
    except:
        LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
        raise
    ## Register our objects with MongoKit
    conn.register([ catalog.Collection, workload.Session ])

    ## Make sure that the databases that we need are there
    db_names = conn.database_names()
    for key in [ 'dataset_db', ]: # FIXME 'workload_db' ]:
        if not config.has_option(configutil.SECT_MONGODB, key):
            raise Exception("Missing the configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
        elif not config.get(configutil.SECT_MONGODB, key):
            raise Exception("Empty configuration option '%s.%s'" % (configutil.SECT_MONGODB, key))
    ## FOR

    ## ----------------------------------------------
    ## MONGODB DATABASE RESET
    ## ----------------------------------------------
    metadata_db = conn[config.get(configutil.SECT_MONGODB, 'metadata_db')]
    dataset_db = conn[config.get(configutil.SECT_MONGODB, 'dataset_db')]

    ## ----------------------------------------------
    ## DUMP DATABASE SCHEMA
    ## ----------------------------------------------

    collections = dict()
    for col_info in metadata_db.Collection.fetch():
        # Skip any collection that doesn't have any documents in it
        if not col_info['doc_count'] or not col_info['avg_doc_size']:
            continue
        collections[col_info['name']] = col_info
    if not collections:
        raise Exception("No collections were found in metadata catalog")
        
    with sys.stdout as fd:
        writer = csv.writer(fd)
        writer.writerow(SCHEMA_COLUMNS)
        for col_name, col_info in collections.iteritems():
            dumpSchema(col_name, col_info["fields"], writer)
            writer.writerow([""]*len(SCHEMA_COLUMNS))
        ## FOR
    ## WITH

    

## MAIN
