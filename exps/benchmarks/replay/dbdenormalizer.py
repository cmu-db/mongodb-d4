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
from workload import WorkloadCombiner
import copy

from dbmigrator import DBMigrator

LOG = logging.getLogger(__name__)

class DBDenormalizer:
	def __init__(self, metadata_db, ori_db, new_db, design):

		self.metadata_db = metadata_db
		self.ori_db = ori_db
		self.new_db = new_db
		self.design = design

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
						ret[doc['name']][doc['fields'][field]['parent_col']] = []
					ret[doc['name']][doc['fields'][field]['parent_col']].append(doc['fields'][field]['parent_key'])

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
	def denormalize(self, graph, parent_keys):
		LOG.info("Denomalizing database")
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
							con_dic = {k: doc[k] for k in f_id}
							# Get the parent document
							p_doc = self.new_db[parent].find(con_dic)
							for i in range(len(f_id)):
								del doc[f_id[i]]

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
	def process(self):
		## step1: copy data from the old_db to new_db
		parent_keys = self.readSchema('schema')
		print parent_keys
		migrator = DBMigrator(self.ori_db, self.new_db)
		migrator.migrate(parent_keys)

		## step2: denormalize the database schema
		graph = self.constructGraph()
		self.denormalize(graph, parent_keys)

		## step3: combine queries
		## TODO
	## DEF

