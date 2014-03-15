import os
import sys
import logging
from pprint import pformat
import time
import copy

# Third-Party Dependencies
basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "../../../libs"))

# mongodb-d4
sys.path.append(os.path.join(basedir, "../../../src"))
sys.path.append(os.path.join(basedir, "../../tools"))

import catalog
import workload
from catalog import Collection
from workload import Session
from util import Histogram
from util import constants
import copy

from dbmigrator import DBMigrator
from dbcombiner import DBCombiner

LOG = logging.getLogger(__name__)

class DBDenormalizer:
    def __init__(self, metadata_db, ori_db, new_db, design):

        self.metadata_db = metadata_db
        self.ori_db = ori_db
        self.new_db = new_db
        self.design = design

        self.processed_session_ids = set()

        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    ## DEF
    def readSchema(self, schema_str):
        LOG.info("Reading Schema information")
        ret = {}
        col = self.metadata_db[schema_str]

        for doc in col.find({},{'_id':False}):
            for field in doc['fields']:
                if not doc['fields'][field]['parent_col'] is None:
                    if not doc['name'] in ret:
                        ret[doc['name']] = {}
                    if not doc['fields'][field]['parent_col'] in ret[doc['name']]:
                        ret[doc['name']][doc['fields'][field]['parent_col']] = {}
                    ret[doc['name']][doc['fields'][field]['parent_col']][field] = doc['fields'][field]['parent_key']

        return ret
    ## DEF

    ## DEF
    def constructGraph(self):
        '''
            Construct a graph based on the design
        '''
        graph = {}

        data = self.design.data

        for k in data:
            p = data[k]['denorm']
            if not k in graph:
                graph[k] = 0
            if not p is None:
                if not p in graph:
                    graph[p] = 1
                else:
                    graph[p] += 1

        return graph
    ## DEF

    ## DEF
    def denormalize(self, input_graph, parent_keys):
        LOG.info("Denomalizing database")

        graph = copy.deepcopy(input_graph) 
        while len(graph) > 0:
            # For each collection that has no embedded collections
            todelete = []
            for key in graph:
                if graph[key] == 0:
                    if not self.design.data[key]['denorm'] is None:
                        # Get its parent collection's name
                        parent = self.design.data[key]['denorm']
                        # Get its parent collection's id (foreign key)
                        f_id = parent_keys[key][parent]
                        #f_id = parent.lower()[0] + u'_id'
                        # For each document in this collection
                        cnt = 1
                        for doc in self.new_db[key].find({},{'_id':False}):
                            print cnt
                            cnt += 1
                            # Get the foreign key's value
                            con_dic = {f_id[k]: doc[k] for k in f_id}
                            # Get the parent document
                            p_doc = self.new_db[parent].find(con_dic)
                            for k in f_id:
                                del doc[k]

                            for pdoc in p_doc:
                                # if this parent document has no this attribute (first embedded)
                                if not key in pdoc:
                                    pdoc[key] = doc
                                # else this parent has already embedded such document before
                                else:
                                    # if it is a dictionary, transform to a list first then append
                                    if isinstance(pdoc[key], dict):
                                        newdic = copy.deepcopy(pdoc[key])
                                        del pdoc[key]
                                        pdoc[key] = []
                                        pdoc[key].append(newdic)
                                        pdoc[key].append(doc)
                                    # if it is already a list, just append
                                    elif isinstance(pdoc[key], list):
                                        pdoc[key].append(doc)
                                # update the parent document 
                                self.new_db[parent].save(pdoc)
                        # drop the child collection
                        self.new_db[key].drop()
                        # update the graph
                        graph[parent] -= 1
                    todelete.append(key)
            for entry in todelete:
                del graph[entry]
    ## DEF

    ## DEF 
    def divideOperations(self):
        LOG.info("Denormalizaing metadata database")
        start_time = time.time()

        workload_cursor = self.metadata_db.sessions.find()
        total_sess = workload_cursor.count()
        processed_sess = 0
        error_sess = 0
        left_sess = total_sess

        LOG.info("Processing %s sessions" % total_sess)
        ## WHILE
        while left_sess > 0:
            if left_sess >= constants.WORKLOAD_WINDOW_SIZE:
                num_to_be_processed = constants.WORKLOAD_WINDOW_SIZE
            else:
                num_to_be_processed = left_sess

            new_workload, num_error, processed_workload_ids = self.combineOps(workload_cursor, num_to_be_processed)
            
            error_sess += num_error
            processed_sess += len(processed_workload_ids)
            left_sess -= num_to_be_processed

            self.processed_session_ids.update(processed_workload_ids)
        ## END WHILE
        LOG.info("Finished metadata denormalization. Total sessions: %s. Error sessions: %s. Processed sessions: %s", total_sess, error_sess, processed_sess)
        LOG.info("Metadata Denormalization takes: %s seconds", time.time() - start_time)
    ## DEF

    ## DEF
    def combineOps(self, cursor, num_to_be_processed):
        sessions = []
        processed_workload_ids = set()
        cnt = 0
        error_sess = 0

        ## WHILE
        while True:
            ## TRY
            try:
                next_sess = cursor.next()
                if next_sess['_id'] in self.processed_session_ids:
                    continue
                
                sessions.append(next_sess)
                processed_workload_ids.add(next_sess['_id'])
                cnt += 1

                if cnt >= num_to_be_processed:
                    break
                break
            except StopIteration:
                break
            except:
                print "Unexpected error:", sys.exc_info()[0]
                cnt += 1
                error_sess += 1
                continue
            ## END TRY
        ## END WHILE

        combiner = DBCombiner(sessions, self.design, self.graph, self.parent_keys)
        combiner.process()
        return sessions, error_sess, processed_workload_ids
    ## DEF

    ## DEF
    def process(self):
        ## step1: copy data from the old_db to new_db
        # self.parent_keys = self.readSchema('schema')
        self.parent_keys = {}
        self.parent_keys['order_line'] = {} 
        self.parent_keys['order_line']['oorder'] = {} 
        self.parent_keys['order_line']['oorder']['ol_w_id'] = 'o_w_id'
        self.parent_keys['order_line']['oorder']['ol_o_id'] = 'o_id'
        self.parent_keys['order_line']['oorder']['ol_d_id'] = 'o_d_id'
        ## print parent_keys
        #migrator = DBMigrator(self.ori_db, self.new_db)
        #migrator.migrate(self.parent_keys)

        ## step2: denormalize the database schema
        ## self.graph = self.constructGraph()
        self.graph = {}
        self.graph['oorder'] = 1
        self.graph['order_line'] = 0
        #self.denormalize(self.graph, self.parent_keys)

        ## step3: combine queries
        ## TODO
        self.divideOperations()
    ## DEF

