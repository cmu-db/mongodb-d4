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

        self.debug = LOG.isEnabledFor(logging.DEBUG)
    ## DEF

    ## DEF
    def combineInserts(self, i_ops):
        graph = copy.deepcopy(self.graph)
        
        error_ops = 0
        ret = []
        updates = []

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
                        if not key in n_ops:
                            todelete.append(key)
                            graph[parent] -= 1
                            continue
                        # Get its parent collection's id (foreign key)
                        dicts = self.parent_keys[key][parent]
                        c_ids = self.parent_keys[key][parent].keys()
                        ## FOR
                        for op in n_ops[key]:
                            # Get the value of these keys in the child op
                            child_values = {f_id:op['query_content'][0][f_id] for f_id in c_ids}

                            flag = False
                            ## IF
                            if parent in n_ops:
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
                            ## END IF
                            # no embedded parent operation found
                            # transform this insert into a update
                            if flag is False:
                                op['type'] = constants.OP_TYPE_UPDATE
                                op['collection'] = parent
                                content = op['query_content'][0]
                                parent_values = {dicts[f_id]:op['query_content'][0][f_id] for f_id in c_ids}
                                for f_id in c_ids:
                                    del content[f_id]
                                op['query_content'][0] = parent_values
                                op['query_content'].append({})
                                op['query_content'][1]['#push'] = {key:content}
                                updates.append(op)
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
        for key in n_ops:
            ret.extend(n_ops[key])
        ## END FOR        
        #for op in ret:
        #    print op
        return ret, error_ops, updates
    ## DEF 

    ## DEF
    def combineDeletes(self, d_ops): 
        graph = copy.deepcopy(self.graph)
        
        error_ops = 0
        ret = []

        updates = []

        n_ops = {}
        for op in d_ops:
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
                        ## IF
                        if not key in n_ops:
                            todelete.append(key)
                            graph[parent] -= 1
                            continue
                        # Get its parent collection's id (foreign key)
                        dicts = self.parent_keys[key][parent]
                        c_ids = self.parent_keys[key][parent].keys()
                        ## FOR
                        for op in n_ops[key]:
                            # Get the value of these keys in the child op
                            child_values = {dicts[f_id]:op['query_content'][0][f_id] for f_id in  (f_id for f_id in c_ids if f_id in op['query_content'][0])}
                            # Get the child predicates
                            child_predicates = {dicts[f_id]:op['predicates'][f_id] for f_id in (f_id for f_id in c_ids if f_id in op['predicates'])}

                            flag = False
                            ## IF
                            if parent in n_ops:
                                # Iterate each parent operation
                                ## FOR
                                for p_op in n_ops[parent]:
                                    # Get the value of these keys in the parent op
                                    parent_values = {dicts[f_id]:p_op['query_content'][0][dicts[f_id]] for f_id in  (f_id for f_id in c_ids if dicts[f_id] in p_op['query_content'][0])}
                                    # Get the parent predicates
                                    parent_predicates = {dicts[f_id]:p_op['predicates'][dicts[f_id]] for f_id in (f_id for f_id in c_ids if dicts[f_id] in p_op['predicates'])}
                                    ## IF
                                    if all(item in child_values.items() for item in parent_values.items()) and all(item in child_predicates.items() for item in parent_predicates.items()):
                                        flag = True
                                        break
                                    ## END IF
                                ## END FOR
                            ## END IF
                            # no embedded parent operation found
                            # transform this insert into a update
                            if flag is False:
                                op['type'] = constants.OP_TYPE_INSERT
                                op['collection'] = parent

                                content = op['query_content'][0]
                                op['query_content'][0] = {}

                                child_query_predicates = op['predicates']
                                op['predicates'] = child_predicates

                                clause_delete = []
                                for c_attr in content:
                                    if c_attr in c_ids:
                                        op['query_content'][0][dicts[c_attr]] = content[c_attr]
                                        clause_delete.append(c_attr)
                                    else:
                                        op['query_content'][0][key+constants.REPLACE_KEY_PERIOD+c_attr] = content[c_attr]
                                        op['predicates'][key+constants.REPLACE_KEY_PERIOD+c_attr] = child_query_predicates[c_attr]
                                for c_attr in clause_delete:
                                    del content[c_attr]

                                if len(op['query_content']) < 2:
                                    op['query_content'].append({})
                                op['query_content'][1]['#pull'] = {key:content}
                                updates.append(op)
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
        for key in n_ops:
            ret.extend(n_ops[key])
        ## END FOR        
        #for op in ret:
        #    print op
        return ret, error_ops, updates

    ## DEF
    def combineQueries(self, ops):
        graph = copy.deepcopy(self.graph)
        #print graph
        
        error_ops = 0
        ret = []

        n_ops = {}
        for op in ops:
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
                        ## IF
                        if not key in n_ops:
                            todelete.append(key)
                            graph[parent] -= 1
                            continue
                        ## ENDIF
                        # Get its parent collection's id (foreign key)
                        dicts = self.parent_keys[key][parent]
                        c_ids = self.parent_keys[key][parent].keys()
                        ## FOR
                        for op in n_ops[key]:
                            # Get the value of these keys in the child op
                            if op['query_content'][0]['#query'] is None:
                                child_values = None
                            else:
                                child_values = {dicts[f_id]:op['query_content'][0]['#query'][f_id] for f_id in  (f_id for f_id in c_ids if f_id in op['query_content'][0]['#query'])}
                            # Get the child predicates
                            child_predicates = {dicts[f_id]:op['predicates'][f_id] for f_id in (f_id for f_id in c_ids if f_id in op['predicates'])}

                            flag = False
                            ## IF
                            if parent in n_ops:
                                # Iterate each parent operation
                                ## FOR
                                for p_op in n_ops[parent]:
                                    # Get the value of these keys in the parent op
                                    parent_values = {dicts[f_id]:p_op['query_content'][0]['#query'][dicts[f_id]] for f_id in  (f_id for f_id in c_ids if dicts[f_id] in p_op['query_content'][0]['#query'])}
                                    # Get the parent predicates 
                                    parent_predicates = {dicts[f_id]:p_op['predicates'][dicts[f_id]] for f_id in (f_id for f_id in c_ids if dicts[f_id] in p_op['predicates'])}
                                    ## IF
                                    # parent where clause should be subset of child where clause
                                    if all(item in child_values.items() for item in parent_values.items()) and all(item in child_predicates.items() for item in parent_predicates.items()): #and all(item in c_ids for item in op['query_content'][0]):
                                        flag = True
                                        p_op['query_fields'][key] = 1
                                        break
                                    ## END IF
                                ## END FOR
                            ## END IF
                            # no embedded parent operation found
                            # transform this query into a query of a parent collection
                            if flag is False:
                                op['collection'] = parent
                                del op['query_fields']
                                op['query_fields'] = {}
                                op['query_fields'][key] = 1

                                child_contents =  op['query_content']
                                op['query_content'] = []
                                op['query_content'].append({})
                                op['query_content'][0]['#query'] = child_values

                                child_query_predicates = op['predicates']
                                op['predicates'] = child_predicates
                                
                                # child collection query has where clause more than foreign keys
                                for c_attr in child_contents[0]['#query']:
                                    if not c_attr in c_ids:
                                        op['query_content'][0]['#query'][key+constants.REPLACE_KEY_PERIOD+c_attr] = child_contents[0]['#query'][c_attr]
                                        op['predicates'][key+constants.REPLACE_KEY_PERIOD+c_attr] = child_query_predicates[c_attr]

                                if not parent in n_ops:
                                    n_ops[parent] = []
                                n_ops[parent].append(op)
                                error_ops += 1
                        ## END FOR
                        del n_ops[key]
                        graph[parent] -= 1
                    ## END IF 
                    todelete.append(key)
                ## END IF
            ## END FOR
            for entry in todelete:
                #print entry
                del graph[entry]
        ## END WHILE
        ## FOR
        for key in n_ops:
            ret.extend(n_ops[key])
        ## END FOR        
        #for op in ret:
        #    print op
        return ret, error_ops

    ## DEF
    def combineUpdates(self, u_ops):
        graph = copy.deepcopy(self.graph)
        
        error_ops = 0
        ret = []

        n_ops = {}
        for op in u_ops:
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
                        ## IF
                        if not key in n_ops:
                            todelete.append(key)
                            graph[parent] -= 1
                            continue
                        # Get its parent collection's id (foreign key)
                        dicts = self.parent_keys[key][parent]
                        c_ids = self.parent_keys[key][parent].keys()
                        ## FOR
                        for op in n_ops[key]:
                            # Get the value of these keys in the child op
                            child_values = {dicts[f_id]:op['query_content'][0][f_id] for f_id in  (f_id for f_id in c_ids if f_id in op['query_content'][0])}
                            # Get the child predicates
                            child_predicates = {dicts[f_id]:op['predicates'][f_id] for f_id in (f_id for f_id in c_ids if f_id in op['predicates'])}

                            op['collection'] = parent
                            where_clause = op['query_content'][0]
                            op['query_content'][0] = {}

                            child_query_predicates = op['predicates']
                            op['predicates'] = child_predicates

                            for c_attr in where_clause:
                                if c_attr in c_ids:
                                    op['query_content'][0][dicts[c_attr]] = where_clause[c_attr]
                                else:
                                    op['query_content'][0][key+constants.REPLACE_KEY_PERIOD+c_attr] = where_clause[c_attr]
                                    op['predicates'][key+constants.REPLACE_KEY_PERIOD+c_attr] = child_query_predicates[c_attr]

                            field_clause = op['query_content'][1]
                            op['query_content'][1] = {}

                            ## FOR
                            for up_op in field_clause:
                                op['query_content'][1][up_op] = {}
                                for k in field_clause[up_op]:
                                    op['query_content'][1][up_op][key+constants.REPLACE_KEY_PERIOD+'0'+constants.REPLACE_KEY_PERIOD+k] = field_clause[up_op][k]
                            ## END FOR

                        ## END FOR
                        if parent not in n_ops:
                            n_ops[parent] = []
                        n_ops[parent].extend(n_ops[key])
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
        for key in n_ops:
            ret.extend(n_ops[key])
        ## END FOR        
        #for op in ret:
        #    print op
        return ret, error_ops
    ## DEF
        
                        
    ## DEF
    def combine(self, session):
        insert_ops = []
        delete_ops = []
        query_ops = []
        update_ops = []
        for op in session['operations']:
            if op['type'] ==constants.OP_TYPE_INSERT:
                #if op['collection'] == 'order_line':
                #    op['query_content'][0]['ol_o_id'] = -1
                insert_ops.append(op)
            elif op['type'] == constants.OP_TYPE_DELETE:
                delete_ops.append(op)
            elif op['type'] == constants.OP_TYPE_QUERY:
                query_ops.append(op)
            elif op['type'] == constants.OP_TYPE_UPDATE:
                update_ops.append(op)
                

        #for op in insert_ops:
        #    print op['collection']

        #for op in update_ops:
        #    print op

        i_ret, errors, i_updates = self.combineInserts(insert_ops)
        update_ops.extend(i_updates)
        session['operations'] = i_ret

        q_ret, errors = self.combineQueries(query_ops)
        session['operations'].extend(q_ret)

        d_ret, errors, d_updates = self.combineDeletes(delete_ops)
        update_ops.extend(d_updates)
        session['operations'].extend(d_ret)

        u_ret, errors = self.combineUpdates(update_ops)
        session['operations'].extend(u_ret)

        return session
    ## DEF

    ## DEF
    def process(self):
        ret = []
        cnt = 0
        for sess in self.sessions:
            ret.append(self.combine(sess))
            #if cnt == 2:
            #    break
            cnt += 1
        return ret
    ## DEF
