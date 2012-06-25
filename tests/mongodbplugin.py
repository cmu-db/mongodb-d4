# -*- coding: utf-8 -*-

import os
import sys
from nose.plugins import Plugin

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../libs"))
sys.path.append(os.path.join(basedir, "../src"))

# Third-Party Dependencies
import mongokit

# mongodb-d4
import catalog
import workload
from util import constants

class MongoDBPlugin(Plugin):
    enabled = True
    hostname = "localhost"
    db_prefix = "test_"

    metadata_db = None
    workload_db = None

    def configure(self, options, conf):
        conn = mongokit.Connection(host=MongoDBPlugin.hostname)
        conn.register([ catalog.Collection, workload.Session ])

        # Drop the databases first
        # Note that we prepend "test_" in front of the db names
        for dbName in [constants.METADATA_DB_NAME, constants.DATASET_DB_NAME]:
            conn.drop_database(MongoDBPlugin.db_prefix + dbName)
        MongoDBPlugin.metadata_db = conn[MongoDBPlugin.db_prefix + constants.METADATA_DB_NAME]
        MongoDBPlugin.dataset_db = conn[MongoDBPlugin.db_prefix + constants.DATASET_DB_NAME]

    ## DEF

    def begin(self):
        pass

## CLASS