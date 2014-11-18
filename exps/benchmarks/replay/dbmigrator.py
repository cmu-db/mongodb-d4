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

from util import Histogram
from util import constants
import copy

LOG = logging.getLogger(__name__)

class DBMigrator:
    def __init__(self, ori_db, new_db):
        self.debug = LOG.isEnabledFor(logging.DEBUG)

        self.ori_db = ori_db
        self.new_db = new_db
    ## DEF

    ## DEF
    def copyData(self, doc, cur_name, parent_keys, docs=[]):
        '''
            doc is a dict
        '''
        #self.new_db[cur_name].insert(doc)   
        #docs = self.new_db[cur_name].find().sort('_id',-1).limit(1)
        #for tmp in docs:
        #   doc = tmp

        for key in doc.keys():
            # Insert into new collection and add the parent's id
            if isinstance(doc[key], dict) and not parent_keys[key] is None and not parent_keys[key][cur_name] is None:

                ## For
                # set the foreign key of the child doc
                for f_id in parent_keys[key][cur_name]:
                    doc[key][f_id] = doc[parent_keys[key][cur_name][f_id]]
                ## END FOR
                
                self.copyData(doc[key], str(key), parent_keys, docs)
                del doc[key]
            elif isinstance(doc[key], list):
                for obj in doc[key]:
                    if isinstance(obj, dict) and not parent_keys[key] is None and not parent_keys[key][cur_name] is None:
                        ## FOR
                        # set the foreign key of the child doc
                        for f_id in parent_keys[key][cur_name]:
                            obj[f_id] = doc[parent_keys[key][cur_name][f_id]]
                        self.copyData(obj, str(key), parent_keys, docs)
                        ## END FOR

                newlist = [x for x in doc[key] if not isinstance(x, dict)]
                doc[key] = newlist
                if len(doc[key]) == 0:
                    del doc[key]

        docs.append(doc)
    ## DEF

    ## DEF
    def migrate(self, parent_keys):
        # Normalization
        LOG.info("Migrating data from old db to new db")
        # TOFIX: collection_names(False):cannot take two arguments?
        for col_name in self.ori_db.collection_names():
            if col_name == 'system.indexes':
                continue
            col = self.ori_db[col_name]
            cnt = 1
            docs = []
            for doc in col.find({},{'_id':False}, timeout=False):
                    #if cnt == 1000:
                    #    break
                    self.copyData(doc, col_name, parent_keys, docs)
                    if cnt % 1000 == 0:
                        self.new_db[col_name].insert(docs)
                        docs = []
                    cnt += 1
            if len(docs) != 0:
                self.new_db[col_name].insert(docs)

