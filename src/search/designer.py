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
import itertools
import logging
import operator
import time
import os
import sys
from pprint import pformat

# mongodb-d4
import workload
import catalog
from initialdesigner import InitialDesigner
from design import Design
from lnsdesigner import LNSDesigner
from randomdesigner import RandomDesigner
from costmodel import CostModel
from util import constants
from util import configutil
from designcandidates import DesignCandidates

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../multithreaded"))

from message import *
import thread

LOG = logging.getLogger(__name__)

## ==============================================
## Designer
## This is the central object that will have all of the
## methods needed to pre-compute the catalog and then
## execute the design search
## ==============================================
class Designer():

    def __init__(self, config, metadata_db, dataset_db, channel=None):
        # SafeConfigParser
        self.config = config

        # The metadata database will contain:
        #   (1) Collection catalog
        #   (2) Workload sessions
        #   (3) Workload stats
        self.metadata_db = metadata_db

        # The dataset database will contain a reconstructed
        # invocation of the database.
        # We need this because the main script will need to
        # compute whatever stuff that it needs
        self.dataset_db = dataset_db

        self.initialSolution = None
        self.finalSolution = None

        # self.page_size = self.config.getint(configutil.SECT_CLUSTER, 'page_size')
        self.page_size = constants.DEFAULT_PAGE_SIZE
        self.sample_rate = self.config.getint(configutil.SECT_DESIGNER, 'sample_rate')

        self.sess_limit = None
        self.op_limit = None

        # Used for multithread
        self.channel = channel
        self.search_method = None
        self.designCandidates = None
        self.collections = None
        self.cm = None
        self.workload = None
        
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def setOptionsFromArguments(self, args):
        """Set the internal parameters of the Designer based on command-line arguments"""

        # HACK HACK HACK HACK
        skip = set(["config", "metadata_db", "dataset_db"])
        for key in args:
            if key in skip: continue
            if self.debug: LOG.debug("%s => %s" % (key, args[key]))
            self.__dict__[key] = args[key]
        ## FOR
        if self.debug: LOG.setLevel(logging.DEBUG)
    ## DEF

    def getCollectionCatalog(self):
        """Return a dict of collection catalog objects"""
        collectionStats = { }
        for stats in self.metadata_db[constants.COLLECTION_SCHEMA].find():
            collectionStats[stats.name] = stats
        return collectionStats
    ## DEF

    ## -------------------------------------------------------------------------
    ## INPUT PROCESSING
    ## -------------------------------------------------------------------------

    def processMongoInput(self, fd, no_load=False, no_post_process=False):
        import inputs.mongodb

        # MongoDB Trace
        converter = inputs.mongodb.MongoSniffConverter(
            self.metadata_db,
            self.dataset_db,
            fd
        )
        converter.stop_on_error = self.stop_on_error
        converter.no_mongo_parse = self.no_mongo_parse
        converter.no_mongo_reconstruct = self.no_mongo_reconstruct
        converter.no_mongo_sessionizer = self.no_mongo_sessionizer
        converter.no_mongo_aggregate_fix = self.no_mongo_aggregate_fix
        converter.no_mongo_normalize = self.no_mongo_normalize
        converter.no_mongo_dependencies = self.no_mongo_dependencies
        converter.random_sessionizer = self.random_sessionizer
        converter.mongo_skip = self.mongo_skip
        converter.sess_limit = self.sess_limit
        converter.op_limit = self.op_limit

        converter.process(
            no_load=no_load,
            no_post_process=no_post_process,
            page_size=self.page_size,
        )
    ## DEF

    def processMySQLInput(self, no_load=False, no_post_process=False):
        from inputs.mysql import MySQLConverter

        # MySQL Trace
        converter = MySQLConverter(
            self.metadata_db,
            self.dataset_db,
            dbHost=self.config.get(configutil.SECT_MYSQL, 'host'),
            dbPort=self.config.getint(configutil.SECT_MYSQL, 'port'),
            dbName=self.config.get(configutil.SECT_MYSQL, 'name'),
            dbUser=self.config.get(configutil.SECT_MYSQL, 'user'),
            dbPass=self.config.get(configutil.SECT_MYSQL, 'pass'))

        converter.no_mysql_schema = self.no_mysql_schema
        converter.no_mysql_workload = self.no_mysql_workload
        converter.no_mysql_dataset = self.no_mysql_dataset
        converter.sess_limit = self.sess_limit
        converter.op_limit = self.op_limit
        
        # Process the inputs and then save the results in mongodb
        converter.process(
            no_load=no_load,
            no_post_process=no_post_process,
            page_size=self.page_size,
        )
    ## DEF

    def generateDesignCandidates(self, collections, isShardingEnabled=True, isIndexesEnabled=True, isDenormalizationEnabled=True):

        dc = DesignCandidates()
        valid_collection = set()
        for col_info in collections.itervalues():

            shardKeys = []
            indexKeys = []
            denorm = []

            interesting = col_info['interesting']
            valid_collection.add(col_info['name'])
            
            interesting = self.__remove_heuristicaly_bad_key__(col_info, interesting)
            # Make sure that none of our interesting fields start with
            # the character that we used to convert $ commands
            for key in interesting:
                assert not key.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX), \
                    "Unexpected candidate key '%s.%s'" % (col_info["name"], key)

            if constants.SKIP_MONGODB_ID_FIELD and "_id" in interesting:
                interesting = interesting[:]
                interesting.remove("_id")

            # deal with shards
            if isShardingEnabled:
                LOG.debug("Sharding is enabled")
                shardKeys = interesting

            # deal with indexes
            if isIndexesEnabled:
                LOG.debug("Indexes is enabled")
                for o in xrange(1, len(interesting) + 1) :
                    if o > constants.MAX_INDEX_SIZE: break
                    for i in itertools.permutations(interesting, o):
                        indexKeys.append(i)
                    ## FOR
                ## FOR
            # deal with de-normalization
            if len(indexKeys) > 10:
                LOG.warn("Too many index keys: %s", len(indexKeys))
            if isDenormalizationEnabled:
                LOG.debug("Denormalization is enabled")
                for k,v in col_info['fields'].iteritems() :
                    if v['parent_col'] <> None and v['parent_col'] not in denorm and v['parent_col'] in valid_collection:
                        denorm.append(v['parent_col'])
            
            dc.addCollection(col_info['name'], indexKeys, shardKeys, denorm)
            ## FOR

        return dc

    def __remove_heuristicaly_bad_key__(self, col_info, keys):
        res = keys[:]
        key_selectivtiy = []
        for key in keys:
            key_selectivtiy.append((col_info['fields'][key]['selectivity'], key))
            if col_info['fields'][key]['selectivity'] < constants.MIN_SELECTIVITY or \
            (col_info['fields'][key]['selectivity'] >= constants.MIN_SELECTIVITY and col_info['fields'][key]['cardinality'] < 3):
                res.remove(key)
            ## IF
        ## FOR
        if len(res) == 0:
            sorted_res = sorted(key_selectivtiy, reverse=True)
            sorted_key = [x[1] for x in sorted_res]
            res = sorted_key[:constants.NUMBER_OF_BACKUP_KEYS]

        return res
    ## DEF

    def loadCollections(self):
        collections = dict()
        for col_info in self.metadata_db.Collection.fetch():
            # Skip any collection that doesn't have any documents in it
            # This is because we won't be able to make any estimates about how
            # big the collection actually is
            if not col_info['doc_count'] or not col_info['avg_doc_size'] or len(col_info['interesting']) == 0 or col_info['workload_queries'] == 0:
                continue
            collections[col_info['name']] = col_info
        ## FOR
        if not collections:
            raise Exception("No collections were found in metadata catalog")
        LOG.info("Loaded %d collections from metadata catalog" % len(collections))

        return collections
    ## DEF

    def loadWorkload(self, collections):
        # We want to bring down all of the sessions that we are going to use to compute the
        # cost of each design
        workload = [ ]
        workloadQuery = {"operations.collection": {"$in": collections.keys()}}
        op_ctr = 0
        cursor = self.metadata_db.Session.fetch(workloadQuery)
        if not self.sess_limit is None:
            assert self.sess_limit >= 0
            cursor.limit(self.sess_limit)
        for sess in cursor:
            if not self.op_limit is None and op_ctr >= self.op_limit:
                break
            workload.append(sess)
            op_ctr += len(sess['operations'])
        ## FOR
        if not len(workload):
            raise Exception("No workload sessions were found in database\n%s" % pformat(workloadQuery))
        LOG.info("Loaded %d sessions with %d operations from workload database", len(workload), op_ctr)
        return workload
    ## DEF

    ## -------------------------------------------------------------------------
    ## DESIGNER EXECUTION
    ## -------------------------------------------------------------------------

    ## HACK HACK HACK
    # the replay flag and replay_design is used to re-evalutated the design read from a design file
    # This is very ugly...but we don't have time now...
    def load(self, replay=False, replay_design=None):
        """Perform the actual search for a design"""
        isShardingEnabled = self.config.getboolean(configutil.SECT_DESIGNER, 'enable_sharding')
        isIndexesEnabled = self.config.getboolean(configutil.SECT_DESIGNER, 'enable_indexes')
        isDenormalizationEnabled = self.config.getboolean(configutil.SECT_DESIGNER, 'enable_denormalization')

        self.collections = self.loadCollections()
        self.workload = self.loadWorkload(self.collections)
        # Generate all the design candidates
        self.designCandidates = self.generateDesignCandidates(self.collections, isShardingEnabled, isIndexesEnabled, isDenormalizationEnabled)
        LOG.info("candidates: %s\n", self.designCandidates)
        # Instantiate cost model
        cmConfig = {
            'weight_network': self.config.getfloat(configutil.SECT_COSTMODEL, 'weight_network'),
            'weight_disk':    self.config.getfloat(configutil.SECT_COSTMODEL, 'weight_disk'),
            'weight_skew':    self.config.getfloat(configutil.SECT_COSTMODEL, 'weight_skew'),
            'nodes':          self.config.getint(configutil.SECT_CLUSTER, 'nodes'),
            'max_memory':     self.config.getint(configutil.SECT_CLUSTER, 'node_memory'),
            'skew_intervals': self.config.getint(configutil.SECT_COSTMODEL, 'time_intervals'),
            'address_size':   self.config.getint(configutil.SECT_COSTMODEL, 'address_size'),
            'window_size':    self.config.getint(configutil.SECT_COSTMODEL, 'window_size')
        }
        self.cm = CostModel(self.collections, self.workload, cmConfig)
#        if self.debug:
#            state.debug = True
#            costmodel.LOG.setLevel(logging.DEBUG)

        # Compute initial solution and calculate its cost
        # This will be the upper bound from starting design
        
        if not replay:
            initialDesign = InitialDesigner(self.collections, self.workload, self.config).generate()
            #initialDesign.reset("CALL_FORWARDING")
            #initialDesign.reset("SPECIAL_FACILITY")
            #initialDesign.reset("SUBSCRIBER")
            #initialDesign.reset("ACCESS_INFO")
            
            #initialDesign.recover("CALL_FORWARDING")
            #initialDesign.recover("SPECIAL_FACILITY")
            #initialDesign.recover("SUBSCRIBER")
            #initialDesign.recover("ACCESS_INFO")
            
            #initialDesign.setDenormalizationParent("ACCESS_INFO", "SUBSCRIBER")
            #initialDesign.setDenormalizationParent("SPECIAL_FACILITY", "SUBSCRIBER")
            #initialDesign.setDenormalizationParent("CALL_FORWARDING", "SPECIAL_FACILITY")
            
            #initialDesign.addIndex("SUBSCRIBER", ["s_id"])
            #initialDesign.addIndex("SUBSCRIBER", ["sub_nbr","s_id"])
            #initialDesign.addShardKey("SUBSCRIBER", ["s_id"])
            
            LOG.info("design\n%s", initialDesign)
            initialCost = self.cm.overallCost(initialDesign)
            return initialCost, initialDesign
        else:
            self.cm.overallCost(replay_design)
            return None
    ## DEF
    
    def search(self, initialCost, initialDesign):
        """
            Main search process starts here
        """
        lock = thread.allocate_lock()
        
        outputfile = self.__dict__.get("output_design", None)
        self.search_method = LNSDesigner(self.collections, self.designCandidates, self.workload, self.config, self.cm, initialDesign, initialCost, self.channel, lock, outputfile)
        self.search_method.start()
    ## DEF

## CLASS