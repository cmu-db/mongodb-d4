
import os, sys
import unittest

import logging
logging.basicConfig(level = logging.INFO,
    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S",
    stream = sys.stdout)

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.realpath(os.path.join(basedir, "../libs")))
sys.path.append(os.path.realpath(os.path.join(basedir, "../src")))

# Third-Party Dependencies
import mongokit

# mongodb-d4
from catalog import Collection
from workload import Session
from util import constants

class MongoDBTestCase(unittest.TestCase):
    """
        Special test case that will automatically setup our connections
        for the metadata and workload databases
    """

    def setUp(self):
        conn = mongokit.Connection()
        conn.register([ Collection, Session ])

        # Drop the databases first
        # Note that we prepend "test_" in front of the db names
        db_prefix = "test_"
        for dbName in [constants.METADATA_DB_NAME, constants.DATASET_DB_NAME]:
            conn.drop_database(db_prefix + dbName)
        self.metadata_db = conn[db_prefix + constants.METADATA_DB_NAME]
        self.dataset_db = conn[db_prefix + constants.DATASET_DB_NAME]

    ## DEF