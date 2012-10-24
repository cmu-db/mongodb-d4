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
from pprint import pformat

# mongodb-d4
import catalog
import workload
from abstractconverter import AbstractConverter
import sql2mongo
from util import *
import utilmethods

LOG = logging.getLogger(__name__)

## ==============================================
## MySQLConverter
## ==============================================
class MySQLConverter(AbstractConverter):
    
    def __init__(self, metadata_db, dataset_db, dbHost, dbPort, dbName, dbUser, dbPass):
        AbstractConverter.__init__(self, metadata_db, dataset_db)

        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbName = dbName
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.mysql_conn = mdb.connect(host=dbHost, port=dbPort, db=dbName, user=dbUser, passwd=dbPass)

        self.next_query_id = 1000l
    ## DEF

    def loadImpl(self):
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

    ## DEF
    
    def extractSchema(self):
        LOG.info("Extracting schema from MySQL")
        
        c1 = self.mysql_conn.cursor()
        c1.execute("""
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s""", self.dbName)
        quick_look = {}
        for row in c1:
            tbl_name = row[0]
            col_info = self.metadata_db.Collection()
            col_info['name'] = tbl_name
            quick_look[col_info['name']] = []
            
            c2 = self.mysql_conn.cursor()
            c2.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME=%s
            """, (self.dbName, tbl_name))
            for col_row in c2:
                col_name = col_row[0]
                quick_look[col_info['name']].append(col_name)
                col_type = catalog.sqlTypeToPython(col_row[1])
                col_type_str = catalog.fieldTypeToString(col_type)
                col_info["fields"][col_name] = catalog.Collection.fieldFactory(col_name, col_type_str)
            ## FOR
            
            # Get the index information from MySQL for this table
            sql = "SHOW INDEXES FROM " + self.dbName + "." + tbl_name
            c3 = self.mysql_conn.cursor()
            c3.execute(sql)
            index_name = None
            # FIXME
            #for ind_row in c3:
                #if index_name <> ind_row[2]:
                    #print pformat(ind_row)
                    #col_info['indexes'][ind_row[2]] = []
                    #index_name = ind_row[2]
                #col_info['indexes'][ind_row[2]].append(ind_row[4])
            ## FOR
            col_info.save()

            col_data = self.dataset_db[tbl_name]
            sql = 'SELECT * FROM ' + self.dbName + '.' + tbl_name
            c4 = self.mysql_conn.cursor()
            c4.execute(sql)
            for data_row in c4 :
                mongo_record = {}
                i = 0
                for column in quick_look[tbl_name] :
                    mongo_record[column] = data_row[i]
                    i += 1
                ## ENDFOR
                col_data.insert(mongo_record)
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
        """, (self.dbName))
        
        for row in c3 :
            rel = row[0].split('.')
            if len(rel) == 4 :
                # rel = [ child_table, child_field, parent_table, parent_field]
                #collection = metadata_db.Collection.find_one({'name' : rel[0]})
                col_info = self.metadata_db.Collection.fetch({"name": rel[0]})

                col_info['fields'][rel[1]]['parent_col'] = rel[2]
                col_info['fields'][rel[1]]['parent_field'] = rel[3]
                col_info['fields'][rel[1]]['parent_conf'] = 1.0
                col_info.save()
    ## DEF
        
    def extractWorkload(self):
        LOG.info("Extracting workload from MySQL query log")

        quick_look = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])

        c4 = self.mysql_conn.cursor()
        c4.execute("""
            SELECT * FROM general_log ORDER BY thread_id, event_time;
        """)

        thread_id = None
        first = True
        uid = 0
        hostIP = utilmethods.detectHostIP()
        mongo = sql2mongo.Sql2Mongo(quick_look)
        for row in c4:
            stamp = float(row[0].strftime("%s"))
            print "row[2]: ", row[2]
            print "thread_id: ", thread_id
            if row[2] <> thread_id :
                thread_id = row[2]
                if not first:
                    if len(session['operations']) > 0 :
                        session.save()
                        uid += 1
                    ## ENDIF
                else :
                    first = False
                ## ENDIF
                session = self.metadata_db.Session()
                session['ip_client'] = utilmethods.stripIPtoUnicode(row[1])
                session['ip_server'] = hostIP
                session['session_id'] = uid
                session['start_time'] = 0.0
                session['end_time'] = 0.0
                session['operations'] = []
            ## ENDIF
            
            if row[5] <> '' :
                sql = re.sub("`", "", row[5])
                success = True
                try:
                    query = mongo.process_sql(sql)
                except (NameError, KeyError, IndexError) as e :
                    success = False
                if success:
                    if mongo.query_type <> 'UNKNOWN' :
                        operations = mongo.generate_operations(stamp)
                        if not len(operations):
                            LOG.warn(row[5])
                        for op in operations:
                            op['query_type'] = mongo.get_op_type(mongo.query_type)
                            op['query_id'] = self.next_query_id
                            session['operations'].append(op)
                            self.next_query_id += 1
                        ## ENDFOR
                    elif row[5].strip().lower() == 'commit' :
                        if len(session['operations']) > 0 :
                            print "start_time: ", session['start_time']
                            session.save()
                            uid += 1
                        ## ENDIF
                        session = self.metadata_db.Session()
                        session['ip_client'] = utilmethods.stripIPtoUnicode(row[1])
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