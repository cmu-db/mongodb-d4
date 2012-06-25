# -*- coding: utf-8 -*-

import itertools
import logging

# mongodb-d4
import catalog
from costmodel import costmodel
import sql2mongo
from parser import MongoSniffConvertor
from search import InitialDesigner
from search import DesignCandidate
from workload import Processor
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
        
    ## DEF

    def setOptionsFromArguments(self, args):
        """Set the internal parameters of the Designer based on command-line arguments"""
        for key in args:
            LOG.debug("%s => %s" % (key, args[key]))
            self.__dict__[key] = args[key]
        ## FOR
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

    def processMongoInput(self, fd):
        # MongoDB Trace
        convertor = MongoSniffConvertor( \
            self.metadata_db, \
            self.dataset_db, \
        )
        convertor.stop_on_error = self.stop_on_error
        convertor.no_mongo_parse = self.no_mongo_parse
        convertor.no_mongo_reconstruct = self.no_mongo_reconstruct
        convertor.no_mongo_sessionizer = self.no_mongo_sessionizer
        convertor.mongo_skip = self.mongo_skip
        convertor.mongo_limit = self.mongo_limit

        convertor.process(fd)
        self.__postProcessInput()
    ## DEF

    def processMySQLInput(self):
        # MySQL Trace
        convertor = sql2mongo.MySQLConvertor( \
            dbHost=self.cparser.get(config.SECT_MYSQL, 'host'), \
            dbPort=self.cparser.getInt(config.SECT_MYSQL, 'port'), \
            dbName=self.cparser.get(config.SECT_MYSQL, 'name'), \
            dbUser=self.cparser.get(config.SECT_MYSQL, 'user'), \
            dbPass=self.cparser.get(config.SECT_MYSQL, 'pass'))

        # Process the inputs and then save the results in mongodb
        convertor.process()
        for collCatalog in convertor.collectionCatalogs():
            self.metadata_db[constants.COLLECTION_SCHEMA].save(collCatalog)
        # TODO: This probably is a bad idea if the sample database
        #       is huge. We will probably want to read tuples one at a time
        #       from MySQL and then write them out immediately to MongoDB
        for collName, collData in convertor.collectionDatasets.iteritems():
            for doc in collData: self.dataset_db[collName].insert(doc)
        for sess in convertor.sessions:
            self.metadata_db[constants.COLLECTION_WORKLOAD].save(sess)

        self.__postProcessInput()
    ## DEF

    def __postProcessInput(self):
        # Now at this point both the metadata and workload collections are populated
        # We can then perform whatever post-processing that we need on them
        processor = Processor(self.metadata_db, self.dataset_db)
        processor.process()

    ## DEF

    ## -------------------------------------------------------------------------
    ## DESIGNER EXECUTION
    ## -------------------------------------------------------------------------

    def generateInitialSolution(self):
        initialDesigner = search.InitialDesigner(self.metadata_db[constants.COLLECTION_SCHEMA].find())
        self.initialSolution = initialDesigner.generate()
        return self.initialSolution
    ## DEF

    def generateDesignCandidate(self):
        dc = search.DesignCandidate()

        for col in self.metadata_db[constants.COLLECTION_SCHEMA].find():
            # deal with shards
            shardKeys = col['interesting']

            # deal with indexes
            indexKeys = [[]]
            for o in xrange(1, len(col['interesting']) + 1) :
                for i in itertools.combinations(col['interesting'], o) :
                    indexKeys.append(i)

            # deal with de-normalization
            denorm = []
            for k,v in col['fields'].iteritems() :
                if v['parent_col'] <> '' and v['parent_col'] not in denorm :
                    denorm.append(v['parent_col'])
            dc.addCollection(col['name'], indexKeys, shardKeys, denorm)
        ## FOR
        return (dc)
    ## DEF

    def search(self):
        cmConfig = {
            'weight_network': self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_network'),
            'weight_disk':    self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_disk'),
            'weight_skew':    self.cparser.getfloat(config.SECT_COSTMODEL, 'weight_skew'),
            'nodes':          self.cparser.getint(config.SECT_CLUSTER, 'nodes'),
            'max_memory':     self.cparser.getint(config.SECT_CLUSTER, 'node_memory'),
            'skew_intervals': self.cparser.getint(config.SECT_COSTMODEL, 'time_intervals'),
            'address_size':   self.cparser.getint(config.SECT_COSTMODEL, 'address_size')
        }

        collections = self.metadata_db[constants.COLLECTION_SCHEMA].find()
        workload = self.metadata_db[constants.COLLECTION_WORKLOAD].find()

        # Instantiate cost model, determine upper bound from starting design
        cm = costmodel.CostModel(collections, workload, cmConfig)
        upper_bound = cm.overallCost(self.initialSolution)
    ## DEF
        
    def generateShardingCandidates(self, collection):
        """Generate the list of sharding candidates for the given collection"""
        assert type(collection) == catalog.Collection
        LOG.info("Generating sharding candidates for collection '%s'" % collection["name"])
        
        # Go through the workload and build a summarization of what fields
        # are accessed (and how often)
        found = 0
        field_counters = { }
        for sess in self.workload_db.Session.find({"operations.collection": collection["name"], "operations.type": ["query", "insert"]}):
            print sess
            
            # For now can just count the number of reads / writes per field
            for op in sess["operations"]:
                for field in op["content"]:
                    if not field in op["content"]: op["content"] = { "reads": 0, "writes": 0}
                    if op["type"] == constants.OP_TYPE_QUERY:
                        field_counters[field]["reads"] += 1
                    elif op["type"] == constants.OP_TYPE_INSERT:
                        # TODO: Should we ignore _id?
                        field_counters[field]["writes"] += 1
                    else:
                        raise Exception("Unexpected query type '%s'" % op["type"])
                ## FOR
            found += 1
        ## FOR
        if not found:
            LOG.warn("No workload sessions exist for collection '%s'" % collection["name"])
            return
            
        return (fields_counters)
    ## DEF

## CLASS