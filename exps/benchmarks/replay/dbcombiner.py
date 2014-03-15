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

LOG = logging.getLogger(__name__)

class DBCombiner:
    ## DEF
    def __init__(self, sessions, design, graph, parent_keys):
        
        self.sessions = sessions
        self.design = design

        self.graph = graph
        self.parent_keys = parent_keys

        self.design.data = {}
        self.design.data['order_line'] = {}
        self.design.data['order_line']['denorm'] = 'oorder'

        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    ## DEF
    def combineInserts(self, i_ops):
        graph = copy.deepcopy(self.graph)
        
        error_ops = 0

        n_ops = {}
        for op in i_ops:
            c_name = op['collection']
            if not c_name in n_ops:
                n_ops[c_name] = []
            n_ops[c_name].append(op)

        ## WHILE        
        while len(graph) > 0:
            todelete = []
            # FOR
            for key in graph:
                ## IF
                if graph[key] == 0:
                    ## IF
                    if key in self.design.data and not self.design.data[key]['denorm'] is None:
                        # Get the parent collection's name
                        parent = self.design.data[key]['denorm']
                        # Get its parent collection's id (foreign key)
                        dicts = self.parent_keys[key][parent]
                        c_ids = self.parent_keys[key][parent].keys()
                        ## FOR
                        for op in n_ops[key]:
                            # Get the value of these keys in the child op
                            child_values = {f_id:op['query_content'][0][f_id] for f_id in c_ids}

                            flag = False
                            # Iterate each parent operation
                            ## FOR
                            for p_op in n_ops[parent]:
                                # Get the value of these keys in the parent op
                                parent_values = {f_id:p_op['query_content'][0][dicts[f_id]] for f_id in c_ids}
                                ## IF
                                if cmp(child_values, parent_values) == 0:
                                    flag = True
                                    ## IF
                                    if not key in p_op['query_content'][0]:
                                        p_op['query_content'][0][key] = []
                                    ## END IF
                                    ## FOR
                                    for f_id in c_ids:
                                       del op['query_content'][0][f_id] 
                                    ## END FOR 
                                    p_op['query_content'][0][key].append(op['query_content'][0])
                                    break
                                ## END IF
                            ## END FOR
                            # no embedded parent operation found
                            if flag is False:
                                error_ops += 1
                        ## END FOR
                        del n_ops[key]
                        graph[parent] -= 1
                    ## END IF 
                    todelete.append(key)
                ## END IF
            ## END FOR
            for entry in todelete:
                del graph[entry]
        ## END WHILE
        ## FOR
        ret = []
        for key in n_ops:
            ret.append(n_ops[key])
        ## END FOR        
        print ret
        return ret, error_ops
    ## DEF 
                        
    ## DEF
    def combine(self, session):
        insert_ops = []
        for op in session['operations']:
            if op['type'] == '$insert':
                insert_ops.append(op)

        for op in insert_ops:
            print op['collection']
        self.combineInserts(insert_ops)
    ## DEF

    ## DEF
    def process(self):
        LOG.info("Combining operations")
        for sess in self.sessions:
            self.combine(sess)
            break
    ## DEF
