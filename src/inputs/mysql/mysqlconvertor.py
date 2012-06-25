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

import re
import logging
import MySQLdb as mdb

# mongodb-d4
import catalog
import workload
from abstractconvertor import AbstractConvertor
from mysql import sql2mongo
from util import *

LOG = logging.getLogger(__name__)

## ==============================================
## MySQLConvertor
## ==============================================
class MySQLConvertor(AbstractConvertor):
    
    def __init__(self, metadata_db, dataset_db, dbHost, dbPort, dbName, dbUser, dbPass):
        AbstractConvertor.__init__(self, metadata_db, dataset_db)

        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbName = dbName
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.mysql_conn = mdb.connect(host=dbHost, port=dbPort, db=dbName, user=dbUser, passwd=dbPass)
        
        self.collectionCatalogs = { }
        self.collectionDatasets = { }
        self.sessions = [ ]
    ## DEF

    def process(self):
        ## ----------------------------------------------
        ## Step 1:
        ## Determine tables/columns/indexes of MySQL schema
        ## ----------------------------------------------
        self.extractSchema()
    
        ## ----------------------------------------------
        ## Step 2:
        ## Query foreign key relationships and store in catalog schema
        ## ----------------------------------------------
        self.extractForeignKeys()
        
        ## ----------------------------------------------
        ## Step 3:
        ## Process MySQL query log for conversion to workload.Session objects
        ## ----------------------------------------------
        self.extractWorkload()

        for collCatalog in convertor.collectionCatalogs():
            self.metadata_db[constants.COLLECTION_SCHEMA].save(collCatalog)
            # TODO: This probably is a bad idea if the sample database
        #       is huge. We will probably want to read tuples one at a time
        #       from MySQL and then write them out immediately to MongoDB
        for collName, collData in convertor.collectionDatasets.iteritems():
            for doc in collData: self.dataset_db[collName].insert(doc)
        for sess in convertor.sessions:
            self.metadata_db[constants.COLLECTION_WORKLOAD].save(sess)

        ## ---------------------------------------------
        ## FIXME Generate Query IDs for the Workload
        ## ---------------------------------------------
        #stats = workload.StatsProcessor(metadata_db, dataset_db)
        #stats.processQueryIds()
    
    ## DEF
    
    def extractSchema(self):
        LOG.info("Extracting schema from MySQL")
        
        c1 = self.mysql_conn.cursor()
        c1.execute("""
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s""", args['name'])
        quick_look = {}
        for row in c1:
            tbl_name = row[0]
            coll_catalog = Collection()
            coll_catalog['name'] = unicode(tbl_name)
            coll_catalog['shard_keys'] = { }
            coll_catalog['fields'] = { }
            coll_catalog['indexes'] = { }
            quick_look[coll_catalog['name']] = []
            
            c2 = self.mysql_conn.cursor()
            c2.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME=%s
            """, (args['name'], tbl_name))
            
            for col_row in c2:
                col_name = col_row[0]
                quick_look[coll_catalog['name']].append(col_name)
                col_type = catalog.sqlTypeToPython(col_row[1])
                coll_catalog["fields"][col_name] = {
                    'type': catalog.fieldTypeToString(col_type),
                    'query_use_count' : 0,
                    'cardinality' : 0,
                    'selectivity' : 0,
                    'parent_col' : '',
                    'parent_field' : '',
                    'parent_conf' : 0.0
                }
            ## FOR
            
            # Get the index information from MySQL for this table
            sql = "SHOW INDEXES FROM " + args['name'] + "." + tbl_name
            c3 = self.mysql_conn.cursor()
            c3.execute(sql)
            index_name = None
            for ind_row in c3:
                if index_name <> ind_row[2]:
                    coll_catalog['indexes'][ind_row[2]] = []
                    index_name = ind_row[2]
                coll_catalog['indexes'][ind_row[2]].append(ind_row[4])
            ## FOR
            coll_catalog.save()
            self.collectionCatalogs[tbl_name] = coll_catalog
            
            coll_data = self.collectionDatasets.get(tbl_name, [])
            # FIXME? coll_data.remove()
            sql = 'SELECT * FROM ' + args['name'] + '.' + tbl_name
            c4 = self.mysql_conn.cursor()
            c4.execute(sql)
            for data_row in c4 :
                mongo_record = {}
                i = 0
                for column in quick_look[tbl_name] :
                    mongo_record[column] = data_row[i]
                    i += 1
                ## ENDFOR
                coll_data.append(mongo_record)
            ## ENDFOR
        ## ENDFOR
    ## DEF


    def extractForeignKeys(self):
        LOG.info("Extracting foreign keys from MySQL")
        
        c3 = self.mysql_conn.cursor()
        c3.execute("""
            SELECT CONCAT( table_name, '.', column_name, '.', 
                referenced_table_name, '.', referenced_column_name ) AS list_of_fks 
            FROM INFORMATION_SCHEMA.key_column_usage 
            WHERE referenced_table_schema = %s 
                AND referenced_table_name IS NOT NULL 
        """, (args['name']))
        
        for row in c3 :
            rel = row[0].split('.')
            if len(rel) == 4 :
                # rel = [ child_table, child_field, parent_table, parent_field]
                #collection = metadata_db.Collection.find_one({'name' : rel[0]})
                collection = self.collectionCatalogs[rel[0]]
                collection['fields'][rel[1]]['parent_col'] = rel[2]
                collection['fields'][rel[1]]['parent_field'] = rel[3]
                collection['fields'][rel[1]]['parent_conf'] = 1.0
                collection.save()
    ## DEF
        
    def extractWorkload(self):
        LOG.info("Extracting workload from MySQL query log")
        
        c4 = self.mysql_conn.cursor()
        c4.execute("""
            SELECT * FROM general_log ORDER BY thread_id, event_time;
        """)
        conn.register([workload.Session])
        metadata_db.drop_collection(constants.COLLECTION_WORKLOAD)
        
        thread_id = None
        first = True
        uid = 0
        hostIP = sql2mongo.detectHostIP()
        mongo = sql2mongo.Sql2Mongo(quick_look)
        for row in c4:
            stamp = float(row[0].strftime("%s"))
            if row[2] <> thread_id :
                thread_id = row[2]
                if first == False :
                    if len(session['operations']) > 0 :
                        session.save()
                        uid += 1
                    ## ENDIF
                else :
                    first = False
                ## ENDIF
                session = metadata_db.Session()
                session['ip_client'] = sql2mongo.stripIPtoUnicode(row[1])
                session['ip_server'] = hostIP
                session['session_id'] = uid
                session['operations'] = []
            ## ENDIF
            
            if row[5] <> '' :
                sql = re.sub("`", "", row[5])
                success = True
                try:
                    query = mongo.process_sql(sql)
                except (NameError, KeyError, IndexError) as e :
                    success = False
                if success == True :
                    if mongo.query_type <> 'UNKNOWN' :
                        operations = mongo.generate_operations(stamp)
                        if len(operations) == 0 :
                            print row[5]
                        for op in operations :
                            session['operations'].append(op)
                        ## ENDFOR
                    elif row[5].strip().lower() == 'commit' :
                        if len(session['operations']) > 0 :
                            session.save()
                            uid += 1
                        ## ENDIF
                        session = self.metadata_db.Session()
                        session['ip_client'] = sql2mongo.stripIPtoUnicode(row[1])
                        session['ip_server'] = hostIP
                        session['session_id'] = uid
                        session['operations'] = []
                    ## ENDIF
                ## ENDIF
            ## End if ##
        ## FOR
        return
    ## DEF

## MAIN