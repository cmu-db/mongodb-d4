# -*- coding: utf-8 -*-

#from workload import *
import logging

# MongoDB-Designer
import catalog
import sql2mongo
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## Designer
## This is the central object that will have all of the
## methods needed to pre-compute the catalog and then
## execute the design search
## ==============================================
class Designer():
    DEFAULT_CONFIG = [
        # MongoDB Trace Processing Options
        ('no-mongo-parse', 'Skip parsing and loading MongoDB workload from file.', False),
        ('no-mongo-reconstruct', 'Skip reconstructing the MongoDB database schema after loading.', False),
        ('no-mongo-sessionizer', 'Skip splitting the MongoDB workload into separate sessions.', False),
        
        # MySQL Processing Options
        ('mysql', 'Whether to process inputs from MySQL', False),
        
        # General Options
        ('stop-on-error', 'Stop processing when an invalid line is reached', False),
    ]

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
        
        for key,desc,default in Designer.DEFAULT_CONFIG:
            self.__dict__[key.replace("-", "_")] = default
        ## FOR
    ## DEF
    

    def processInput(self):
        # MongoDB Trace
        if not self.mysql:
            convertor = parser.MongoSniffConvertor( \
                self.metadata_db, \
                self.dataset_db, \
            )
            convertor.process()
        
        # MySQL Trace
        else:
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
            # is huge. We will probably want to read tuples one at a time
            # from MySQL and then write them out immediately to MongoDB
            for collName, collData in convertor.collectionDatasets.iteritems():
                for doc in collData: self.dataset_db[collName].insert(doc)
            for sess in convertor.sessions:
                self.metadata_db[constants.COLLECTION_WORKLOAD].save(sess)
                
        # Now at this point both the metadata and workload collections are populated
        # We can then perform whatever post-processing that we need on them
        processor = workload.Processor(self.metadata_db, self.dataset_db)
        processor.process()
        
    ## FOR
        
        
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
                    if op["type"] == "query":
                        field_counters[field]["reads"] += 1
                    elif op["type"] == "insert":
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