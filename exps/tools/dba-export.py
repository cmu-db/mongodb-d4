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

import os, sys
import argparse
import logging
import random
import csv
import re
import string
import collections
import numpy
from pprint import pformat
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
from util.histogram import Histogram

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
    "list_len_min",
    "list_len_max",
    "list_len_avg",
    "list_len_stdev",
]
STATS_COLUMNS = [
    "collection",
    "doc_count",
    "avg_doc_size",
    "data_size",
    "workload_queries",
    "workload_percent",
]
    
STRIP_FIELDS = [
    "predicates",
    "query_hash",
    "query_time",
    "query_size",
    "query_type",
    "query_id",
    "orig_query",
    "resp_.*",
]
STRIP_REGEXES = [ re.compile(r) for r in STRIP_FIELDS ]

QUERY_COUNTS = Histogram()
QUERY_COLLECTION_COUNTS = Histogram()
QUERY_HASH_XREF = { }
QUERY_TOP_LIMIT = 10

## ==============================================
## DUMP SCHEMA
## ==============================================
def dumpSchema(writer, collection, fields, spacer=""):
    cur_spacer = spacer
    if len(spacer) > 0: cur_spacer += " - "
    for f_name in sorted(fields.iterkeys(), key=lambda x: x != "_id"):
        row = [ ]
        f = fields[f_name]
        for key in SCHEMA_COLUMNS:
            if key == SCHEMA_COLUMNS[0]:
                val = collection.replace("__", "_")
            elif key == SCHEMA_COLUMNS[1]:
                val = cur_spacer + f_name
            else:
                val = f.get(key, "")
                if val is None: val = ""
            row.append(val)
        writer.writerow(row)
        
        if len(f.get("fields", [])) > 0:
            dumpSchema(writer, collection, f["fields"], spacer+"  ")
    ## FOR
## DEF

## ==============================================
## DUMP COLLECTION STATS
## ==============================================
def dumpStats(writer, col_info, TOTAL_DB_SIZE):
    row = [ ]
    #print pformat(col_info)
    for key in STATS_COLUMNS:
        if key == STATS_COLUMNS[0]:
            val = col_info["name"].replace("__", "_")
        elif key == "data_size":
            val = "%.1f%%" % ((col_info[key] / float(TOTAL_DB_SIZE))*100)
        elif key == "workload_percent":
            if "workload_percent" in col_info:
                val = "%.1f%%" % (col_info[key]*100)
            else:
                val = "-"
        else:
            assert key in col_info, key
            val = col_info[key]
        row.append(val)
    ## FOR
    writer.writerow(row)
    return
## DEF

## ==============================================
## DUMP OP
## ==============================================
def dumpOp(fd, op):
    # Remove all of the resp_* fields
    for k in op.keys():
        for regex in STRIP_REGEXES:
            if regex.match(k):
                del op[k]
                break
        if op["type"] != constants.OP_TYPE_UPDATE:
            for k in ["update_multi", "update_upsert"]:
                if k in op: del op[k]
        ## IF
        if "query_aggregate" in op and op["query_aggregate"] == False:
            del op["query_aggregate"]
            
    ## FOR
    
    # Get $in stats if we have them
    inHistogram = None
    for op_contents in workload.getOpContents(op):
        inHistogram = computeInStats(op_contents)
        if not inHistogram is None:
            # We need to compute it for all other ops
            # with the same hash
            for other_op in QUERY_HASH_XREF[hash]:
                if other_op == op: continue
                for op_contents in workload.getOpContents(other_op):
                    computeInStats(op_contents, inHistogram)
            break
    ## FOR
    
    contents = pformat(convert(op))
    fd.write("Query Count: %.1f%%\n" % percentage)
    fd.write(contents + "\n")
    if not inHistogram is None:
        all_values = inHistogram.getAllValues()
        fd.write("\n$IN STATISTICS:\n")
        fd.write("  + min len: %d\n" % min(all_values))
        fd.write("  + max len: %d\n" % max(all_values))
        fd.write("  + avg len: %.2f\n" % numpy.average(all_values))
        fd.write("  + stdev:   %.2f\n" % numpy.std(all_values))
    return
## DEF
    

## ==============================================
## http://stackoverflow.com/a/1254499/42171
## ==============================================
CMD_FIX_REGEX = re.compile("^\#([\w]+)")
def convert(data):
    if isinstance(data, unicode):
        return CMD_FIX_REGEX.sub("$\\1", str(data).replace("__", "_")) # HACK
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data
        
def computeInStats(query, h=None):
    for k,v in query.iteritems():
        if k == "#in":
            if h is None: h = Histogram()
            h.put(len(v))
        elif isinstance(v, list):
            for inner in v:
                if isinstance(inner, dict):
                    h = computeInStats(inner, h)
        elif isinstance(v, dict):
            h = computeInStats(v, h)
    return h

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s - Distributed Document Database Designer" % constants.PROJECT_NAME)
                                      
    # Configuration File Options
    aparser.add_argument('project', help='Project Name')
    aparser.add_argument('--config', type=file, help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--op-limit', type=int, metavar='N', help='The number of operations to include in the sample workload', default=QUERY_TOP_LIMIT)
    aparser.add_argument('--no-schema', action='store_true', help='Disable generating schema file.')
    aparser.add_argument('--no-stats', action='store_true', help='Disable generating collection stats file.')
    aparser.add_argument('--no-workload', action='store_true', help='Disable generating sample query file.')
    aparser.add_argument('--debug', action='store_true', help='Enable debug log messages.')
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
    
    if not os.path.exists(args['project']):
        os.mkdir(args['project'])
    
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

    colls = dict()
    for col_info in metadata_db.Collection.fetch({"workload_queries": {"$gt": 0}}):
        # Skip any collection that doesn't have any documents in it
        if not col_info['doc_count'] or not col_info['avg_doc_size']:
            continue
        colls[col_info['name']] = col_info
    if not colls:
        raise Exception("No collections were found in metadata catalog")
    
    for sess in metadata_db.Session.fetch():
        for op in sess["operations"]:
            QUERY_COUNTS.put(op["query_hash"])
            if not op["query_hash"] in QUERY_HASH_XREF:
                QUERY_HASH_XREF[op["query_hash"]] = [ ]
            QUERY_HASH_XREF[op["query_hash"]].append(op)
            QUERY_COLLECTION_COUNTS.put(op["collection"])
        ## FOR
    ## FOR

    TOTAL_DB_SIZE = sum([col_info["data_size"] for col_info in colls.itervalues()])
    LOG.debug("Estimated Total Database Size: %d" % TOTAL_DB_SIZE)
    TOTAL_QUERY_COUNT = QUERY_COLLECTION_COUNTS.getSampleCount()
    LOG.debug("Total # of Queries: %d" % TOTAL_QUERY_COUNT)
    
    # HACK: Fix collections
    for col_name, col_info in colls.iteritems():
        col_info["workload_queries"] = QUERY_COLLECTION_COUNTS.get(col_name)
        col_info["workload_percent"] = QUERY_COLLECTION_COUNTS.get(col_name) / float(TOTAL_QUERY_COUNT)
        try:
            col_info.save()
        except Exception as ex:
            LOG.warn(ex)
            pass
    ## FOR
    
    ## ----------------------------------------------
    ## DUMP DATABASE SCHEMA
    ## ----------------------------------------------
    if not args["no_schema"]:
        LOG.debug("Dumping schema catalog")
        outputFile = os.path.join(args["project"], "%s-schema.csv" % args["project"])
        with open(outputFile, "w") as fd:
            writer = csv.writer(fd)
            writer.writerow(map(string.upper, SCHEMA_COLUMNS))
            for col_name, col_info in colls.iteritems():
                dumpSchema(writer, col_name, col_info["fields"])
                writer.writerow([""]*len(SCHEMA_COLUMNS))
            ## FOR
        ## WITH
        LOG.info("Created Schema File: %s", outputFile)
    else:
        LOG.info("Skipping Schema File")
        
    ## ----------------------------------------------
    ## DUMP COLLECTION STATS
    ## ----------------------------------------------
    if not args["no_stats"]:
        LOG.debug("Dumping collection stats")
        outputFile = os.path.join(args["project"], "%s-stats.csv" % args["project"])
        with open(outputFile, "w") as fd:
            writer = csv.writer(fd)
            writer.writerow(map(string.upper, STATS_COLUMNS))
            for col_info in colls.itervalues():
                dumpStats(writer, col_info, TOTAL_DB_SIZE)
            ## FOR
        ## WITH
        LOG.info("Created Collection Statistics File: %s", outputFile)
    else:
        LOG.info("Skipping Collection Statistics File")

    ## ----------------------------------------------
    ## DUMP WORKLOAD
    ## ----------------------------------------------
    if not args["no_workload"]:
        LOG.debug("Dumping sample queries")
        for op_per_collection in [True, False]:
            limit = len(colls) if op_per_collection else args['op_limit']
            assert limit >= 0
            prefix = "-top%d"%limit if not op_per_collection else ""
            outputFile = os.path.join(args["project"], "%s%s-queries.txt" % (args["project"], prefix))
            with open(outputFile, "w") as fd:
                first = True
                total_queries = QUERY_COUNTS.getSampleCount()
                op_collections = set()
                for hash in sorted(QUERY_COUNTS.keys(), key=lambda x: QUERY_COUNTS[x], reverse=True):
                    percentage = (QUERY_COUNTS[hash] / float(total_queries)) * 100
                    op = random.choice(QUERY_HASH_XREF[hash])
                    
                    # Skip any queries on a non-data table
                    if op["collection"] in constants.IGNORED_COLLECTIONS or op["collection"].endswith("$cmd"):
                        continue
                    
                    if op_per_collection and op["collection"] in op_collections:
                        continue
                    op_collections.add(op["collection"])
                    
                    if not first: fd.write("\n%s\n\n" % ("-"*100))
                    
                    # HACK
                    op["collection"] = op["collection"].replace("__", "_")
                    
                    # Dump out the op
                    dumpOp(fd, op)
                    
                    if limit == 0: break
                    limit -= 1
                    first = False
                ## FOR
            ## WITH
            if op_per_collection:
                # Print a warning if we don't have any ops for some collections
                missing = set(colls.keys()) - op_collections
                if len(missing) > 0:
                    LOG.warn("Missing Ops for Collections:\n%s" % "".join(map(lambda x: "  - %s\n" % x, missing)))
                LOG.info("Created Collection Query Sample File: %s", outputFile)
            else:
                LOG.info("Created Top %d Query Sample File: %s", args['op_limit'], outputFile)
        ## FOR
    else:
        LOG.info("Skipping Query Sample File")

## MAIN
