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
import time

LOG = logging.getLogger(__name__)

MYSQL_LOG_TABLE_NAME = "general_log"

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
        self.mysql_conn = mdb.connect(host=dbHost, port=dbPort, db=dbName, user=dbUser, passwd=dbPass, charset='utf8')
        self.next_query_id = 1000l
        
        self.no_mysql_schema = False
        self.no_mysql_workload = False
        self.no_mysql_dataset = False
        
        self.rng = random.Random()

        # LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    def loadImpl(self):
        ## ----------------------------------------------
        ## Step 1:
        ## Determine tables/columns/indexes of MySQL schema
        ## ----------------------------------------------
        if not self.no_mysql_schema:
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
        if not self.no_mysql_workload:
            self.extractWorkload()
    
    ## DEF
    
    def postProcessImpl(self):
        # Nothing!
        pass
    ## DEF

    def extractSchema(self):
        c1 = self.mysql_conn.cursor()
        c1.execute("""
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME != %s""", \
            (self.dbName, MYSQL_LOG_TABLE_NAME))
        tbl_cols = {}
        LOG.info("Extracting table information from database '%s'", self.dbName)
        for row in c1:
            tbl_name = row[0]
            col_info = self.metadata_db.Collection()
            col_info['name'] = tbl_name
            tbl_cols[col_info['name']] = []
            if self.debug: LOG.debug("Created metadata object for collection '%s'", tbl_name)

            c2 = self.mysql_conn.cursor()
            c2.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME=%s
            """, (self.dbName, tbl_name))
            for col_row in c2:
                col_name = col_row[0]
                tbl_cols[col_info['name']].append(col_name)
                col_type = catalog.sqlTypeToPython(col_row[1])
                col_type_str = catalog.fieldTypeToString(col_type)
                col_info["fields"][col_name] = catalog.Collection.fieldFactory(col_name, col_type_str)
                LOG.debug("Created column information for '%s.%s'", tbl_name, col_name)
            ## FOR

            # Get the index information from MySQL for this table
            sql = "SHOW INDEXES FROM " + self.dbName + "." + tbl_name
            c3 = self.mysql_conn.cursor()
            c3.execute(sql)
            index_name = None
            LOG.info("Extracting index information from table '%s'", tbl_name)
            # FIXME
            #for ind_row in c3:
                #if index_name <> ind_row[2]:
                    #print pformat(ind_row)
                    #col_info['indexes'][ind_row[2]] = []
                    #index_name = ind_row[2]
                #col_info['indexes'][ind_row[2]].append(ind_row[4])
            ## FOR
            col_info.save()

            ## -----------------------------------------------------------
            ## EXTRACT DATA
            ## -----------------------------------------------------------
            if not self.no_mysql_dataset:
                self.extractData(tbl_name, tbl_cols[tbl_name])

        ## ENDFOR
    ## DEF

    def extractData(self, tbl_name, tbl_cols, batchSize=100):
        assert isinstance(tbl_cols, list)

        cur = self.mysql_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM %s.%s" % (self.dbName, tbl_name))
        row_total = cur.fetchone()[0]
        cur.close()

        LOG.info("Copying %d rows from table '%s' into MongoDB", row_total, tbl_name)
        col_data = self.dataset_db[tbl_name]
        c4 = self.mysql_conn.cursor()
        c4.execute("SELECT * FROM %s.%s" % (self.dbName, tbl_name))
        row_ctr = 0
        batch = [ ]
        for data_row in c4 :
            mongo_record = dict((tbl_cols[i], data_row[i]) for i in xrange(len(tbl_cols)))
            batch.append(mongo_record)
            row_ctr += 1

            if len(batch) >= batchSize:
                if self.debug: LOG.debug("Inserting new batch with %d records to %s [%d / %d]", len(batch), tbl_name, row_ctr, row_total)
                try:
                    col_data.insert(batch)
                except:
                    LOG.warn("Failed to insert data into '%s'\n%s", tbl_name, pformat(batch))
                    raise
                batch = [ ]
        ## ENDFOR
        if len(batch) > 0:
            if self.debug: LOG.debug("Inserting new batch with %d records to %s [%d / %d]", len(batch), tbl_name, row_ctr, row_total)
            try:
                col_data.insert(batch)
            except:
                LOG.warn("Failed to insert data into '%s'\n%s", tbl_name, pformat(batch))
                raise
        assert row_total == row_ctr
        LOG.info("Sucessfully copied %d rows from table '%s' into MongoDB", row_ctr, tbl_name)
    ## DEF

    def extractForeignKeys(self):
        LOG.info("Extracting foreign keys from MySQL")

        sql = """
        SELECT CONCAT( table_name, '.', column_name, '.',
        referenced_table_name, '.', referenced_column_name ) AS list_of_fks
        FROM INFORMATION_SCHEMA.key_column_usage
        WHERE referenced_table_schema = %s
        AND referenced_table_name IS NOT NULL
        """
        c3 = self.mysql_conn.cursor()
        c3.execute(sql, (self.dbName))

        for row in c3 :
            rel = tuple(row[0].split('.'))
            if self.debug: LOG.debug("Foreign Key: %s", rel)
            if len(rel) == 4 :
                child_table, child_field, parent_table, parent_field = rel
                col_info = self.metadata_db.Collection.fetch_one({"name": child_table})
                assert child_field in col_info['fields']
                field = col_info['fields'][child_field]
                field['parent_col'] = parent_table
                field['parent_key'] = parent_field
                
                col_info.save()
    ## DEF

    def extractWorkload(self):
        LOG.info("Extracting workload from MySQL query log")

        cur = self.mysql_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM %s.%s" % (self.dbName, MYSQL_LOG_TABLE_NAME))
        row_total = cur.fetchone()[0]
        cur.close()

        # All the timestamps should be relative to the first timestamp
        cur = self.mysql_conn.cursor()
        cur.execute("SELECT MIN(event_time) FROM %s.%s" % (self.dbName, MYSQL_LOG_TABLE_NAME))
        start_timestamp = float(cur.fetchone()[0].strftime("%s"))
        cur.close()
        if self.debug: LOG.debug("Workload Start Timestamp: %s", start_timestamp)

        c4 = self.mysql_conn.cursor()
        c4.execute("SELECT * FROM %s ORDER BY thread_id, event_time" % MYSQL_LOG_TABLE_NAME)

        thread_id = None
        first = True
        uid = 0
        hostIP = utilmethods.detectHostIP()
        tbl_cols = dict([ (c['name'], c) for c in self.metadata_db.Collection.fetch()])
        mongo = sql2mongo.Sql2Mongo(tbl_cols)
        query_ctr = 0
        for row in c4:
            timestamp = float(row[0].strftime("%s")) - start_timestamp

            if row[2] <> thread_id :
                thread_id = row[2]
                if not first:
                    if len(session['operations']) > 0:
                        if session['start_time'] is None:
                            session['start_time'] = session['operations'][0]['query_time']
                        if session['end_time'] is None:
                            session['end_time'] = session['operations'][-1]['query_time']
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
                session['start_time'] = None
                session['end_time'] = None
                session['operations'] = []
            ## ENDIF

            if row[5] <> '' :
                sql = re.sub("`", "", row[5])
                success = True
                try:
                    query = mongo.process_sql(sql)
                except (NameError, KeyError, IndexError) as e :
                    success = False
                except Exception as e:
                    LOG.error("Failed to process SQL:\n" + sql)
                    success = False
                    pass
                    #raise
                finally:
                    query_ctr += 1
                    if query_ctr % 50000 == 0:
                        LOG.info("Processed %d / %d queries [%d%%]", query_ctr, row_total, 100*query_ctr/float(row_total))

                if success:
                    if mongo.query_type <> 'UNKNOWN' :
                        operations = mongo.generate_operations(timestamp)
                        if operations is None or not len(operations):
                            LOG.warn("SKIP: %s", row[5])
                            continue
                        for op in operations:
                            op['orig_query'] = sql
                            op['type'] = mongo.get_op_type(mongo.query_type)
                            op['query_id'] = self.next_query_id
                            session['operations'].append(op)
                            if session['start_time'] is None and op['query_time']:
                                session['start_time'] = op['query_time']
                            session['end_time'] = op['query_time']
                            self.next_query_id += 1
                        ## ENDFOR
                    elif row[5].strip().lower() == 'commit' :
                        if len(session['operations']) > 0 :
                            #if self.debug: LOG.debug("start_time: %s", session['start_time'])
                            if session['start_time'] is None:
                                session['start_time'] = session['operations'][0]['query_time']
                            if session['end_time'] is None:
                                session['end_time'] = session['operations'][-1]['query_time']
                            session.save()
                            uid += 1
                        ## ENDIF
                        session = self.metadata_db.Session()
                        session['ip_client'] = utilmethods.stripIPtoUnicode(row[1])
                        session['ip_server'] = hostIP
                        session['session_id'] = uid
                        session['start_time'] = None
                        session['end_time'] = None
                        session['operations'] = []
                    ## ENDIF
                ## ENDIF
            ## End if ##
        ## FOR
        return
    ## DEF

## MAIN
