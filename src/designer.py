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

# mongodb-d4
from pprint import pformat
import catalog
from costmodel import costmodel
from search import InitialDesigner, lnsearch
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## Designer
## This is the central object that will have all of the
## methods needed to pre-compute the catalog and then
## execute the design search
## ==============================================
class Designer():

    def __init__(self, cparser, metadata_db, dataset_db):
        # SafeConfigParser
        self.cparser = cparser
        
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

        # self.page_size = self.cparser.getint(config.SECT_CLUSTER, 'page_size')
        self.page_size = constants.DEFAULT_PAGE_SIZE
        self.sample_rate = self.cparser.getint(config.SECT_DESIGNER, 'sample_rate')

        self.sess_limit = None
        self.op_limit = None

        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def setOptionsFromArguments(self, args):
        """Set the internal parameters of the Designer based on command-line arguments"""
        for key in args:
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
            dbHost=self.cparser.get(config.SECT_MYSQL, 'host'),
            dbPort=self.cparser.getint(config.SECT_MYSQL, 'port'),
            dbName=self.cparser.get(config.SECT_MYSQL, 'name'),
            dbUser=self.cparser.get(config.SECT_MYSQL, 'user'),
            dbPass=self.cparser.get(config.SECT_MYSQL, 'pass'))

        # Process the inputs and then save the results in mongodb
        converter.process(
            no_load=no_load,
            no_post_process=no_post_process,
            page_size=self.page_size,
        )
    ## DEF

    ## -------------------------------------------------------------------------
    ## DESIGNER EXECUTION
    ## -------------------------------------------------------------------------

    def search(self):
        """Perform the actual search for a design"""
        cmConfig = {
            'weight_network': self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_network'),
            'weight_disk':    self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_disk'),
            'weight_skew':    self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_skew'),
            'nodes':          self.cparser.getint(config.SECT_CLUSTER, 'nodes'),
            'max_memory':     self.cparser.getint(config.SECT_CLUSTER, 'node_memory'),
            'skew_intervals': self.cparser.getint(config.SECT_COSTMODEL, 'time_intervals'),
            'address_size':   self.cparser.getint(config.SECT_COSTMODEL, 'address_size')
        }

        collectionsDict = dict()
        for col_info in self.metadata_db.Collection.fetch():
            # Skip any collection that doesn't have any documents in it
            # This is because we won't be able to make any estimates about how
            # big the collection actually is
            if not col_info['doc_count'] or not col_info['avg_doc_size']:
                continue
            collectionsDict[col_info['name']] = col_info
        ## FOR
        if not collectionsDict:
            raise Exception("No collections were found in metadata catalog")
        LOG.info("Loaded %d collections from metadata catalog" % len(collectionsDict))

        # We want to bring down all of the sessions that we are going to use to compute the
        # cost of each design
        workload = [ ]
        workloadQuery = {"operations.collection": {"$in": collectionsDict.keys()}}
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

        # Instantiate cost model
        cm = costmodel.CostModel(collectionsDict, workload, cmConfig)
#        if self.debug:
#            state.debug = True
#            costmodel.LOG.setLevel(logging.DEBUG)

        # Compute initial solution and calculate its cost
        # This will be the upper bound from starting design
        initialDesign = InitialDesigner(collectionsDict.values()).generate()
        upper_bound = cm.overallCost(initialDesign)
        if self.debug:
            LOG.debug("Initial Design\n%s", initialDesign)
            LOG.debug("Computed initial design [COST=%f]", upper_bound)

#        cm.debug = True
#        costmodel.LOG.setLevel(logging.DEBUG)
        LOG.info("Executing D4 search algorithm...")
        ln = lnsearch.LNSearch(self.cparser, collectionsDict, cm, initialDesign, upper_bound, 10)
        solution = ln.solve()
        return solution
    ## DEF

## CLASS