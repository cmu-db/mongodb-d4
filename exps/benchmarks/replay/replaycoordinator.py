# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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

import sys
import os
import logging
from pprint import pprint, pformat
import copy

LOG = logging.getLogger(__name__)

basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "tools"))

import pymongo

from dbdenormalizer import DBDenormalizer
from design_deserializer import Deserializer

from api.abstractcoordinator import AbstractCoordinator
from api.message import *

class ReplayCoordinator(AbstractCoordinator):
    DEFAULT_CONFIG = [
        ("ori_db", "Name of the database that contains the original data (Change None to valid dataset name)", "None"),
        ("new_db", "Name of the database that we will copy the 'original database' into using the new design.", "None"),
        ("metadata", "Name of the metadata replay will execute (Change None to valid metadata name)", "None"),
        ("new_meta", "Name of the new metadata database that replay will execute", "None"),
        ("metadata_host", "The host name for metadata database", "localhost:27017"),
     ]
    
    def benchmarkConfigImpl(self):
        return self.DEFAULT_CONFIG
    ## DEF

    def initImpl(self, config, channels):
        metadata_conn = None
        targetHost = config['replay']['metadata_host']
        try:
            metadata_conn = pymongo.Connection(targetHost)
        except:
            LOG.error("Failed to connect to target MongoDB at %s", targetHost)
            raise
        assert metadata_conn

        self.metadata_db = metadata_conn[config['replay']['metadata']]
        self.new_meta = metadata_conn[config['replay']['new_meta']]
        self.ori_db = self.conn[self.config['replay']['ori_db']]
        self.new_db = self.conn[self.config['replay']['new_db']]
        self.design = self.getDesign(self.config['default']['design'])
        return dict()
    ## DEF
    
    def loadImpl(self, config, channels):
        if self.config['default']['reset']:
            LOG.info("Resetting database %s" % self.config['replay']['new_db'])
            self.conn.drop_database(self.new_db)
            self.conn.drop_database(self.new_meta)
        self.prepare()
        return dict()
    ## DEF    

    def prepare(self):
        # Denormalization 
        # STEP 1: Reconstruct database and workload based on the given design
        LOG.info("Denormalizing database")
        d = DBDenormalizer(self.metadata_db, self.new_meta, self.ori_db, self.new_db, self.design)
        d.process()

        # STEP 2: Put indexs on the dataset_db based on the given design
        self.setIndexes(self.new_db, self.design)
    ## DEF
    
    def setIndexes(self, new_db, design):
        LOG.info("Creating indexes")
        for col_name in design.getCollections():
            new_db[col_name].drop_indexes()
            
            indexes = design.getIndexes(col_name)
            # The indexes is a list of tuples
            for tup in indexes:
                index_list = [ ]
                for element in tup:
                    index_list.append((str(element), pymongo.ASCENDING))
                ## FOR
                try:
                    new_db[col_name].ensure_index(index_list)
                except:
                    LOG.error("Failed to create indexes on collection %s", col_name)
                    LOG.error("Indexes: %s", index_list)
                    raise
            ## FOR
        ## FOR
        LOG.info("Finished indexes creation")
    ## DEF
    
    def setupShardKeys(self):
        LOG.info("Creating shardKeys")
        design = self.design
        admindb = self.conn["admin"]
        db = self.new_db

        assert admindb != None
        assert db != None
        # Enable sharding on the entire database
        try:
            # print {"enablesharding": db.name}
            result = admindb.command({"enablesharding": db.name})
            assert result["ok"] == 1, "DB Result: %s" % pformat(result)
        except:
            LOG.error("Failed to enable sharding on database '%s'" % db.name)
            raise

        for col_name in design.getCollections():
            shardKeyDict = { }
            
            for key in design.getShardKeys(col_name):
                shardKeyDict[key] = 1
            ## FOR

            # Continue if there are no shardKeys for this collection
            if len(shardKeyDict) == 0:
                continue
            
            # add shard keys
            try: 
                # print {"shardcollection" : str(db.name) + "." + col_name, "key" : shardKeyDict}
                admindb.command({"shardcollection" : str(db.name) + "." + col_name, "key" : shardKeyDict})
            except: 
                LOG.error("Failed to enable sharding on collection '%s.%s'" % (db.name, col_name))
                LOG.error("Command ran: %s", {"shardcollection" : str(db.name) + "." + col_name, "key" : shardKeyDict})
                raise
        ## FOR
        LOG.info("Successfully create shardKeys on collections")
    ## DEF

    def getDesign(self, design_path):
        assert design_path, "design path is empty"

        deserializer = Deserializer(design_path)
        
        design = deserializer.Deserialize()
        LOG.info("current design \n%s" % design)

        return design
    ## DEF

    ## DEF
    def executeImpl(self, config, channels):
        cnt = self.new_meata[COLLECTION_WORKLOAD].count()
        num = len(channels)
        range_size = cnt/num
        start = 0
        ret = []

        while start <= cnt:
            end = start + range_size
            if end > cnt:
                end = cnt + 1
            ret.append((start, end))

            start += range_size 

        return ret
    ## DEF
## CLASS
