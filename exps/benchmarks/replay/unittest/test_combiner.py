import os
import sys
import pymongo

basedir = os.getcwd()

# Third-party Dependencies
sys.path.append(os.path.join(basedir, "../../../../libs"))
sys.path.append(os.path.join(basedir, "../../../../src"))
sys.path.append(os.path.join(basedir, "../../../tools"))
sys.path.append(os.path.join(basedir, "../../../../src/search"))

# mongo-d4-benchmark-replay
sys.path.append(os.path.join(basedir, ".."))

from dbcombiner import DBCombiner
from dbdenormalizer import DBDenormalizer
from design_deserializer import Deserializer
from design import Design

def test_combine_deletes(combiner, operations):
    return combiner.combineDeletes(operations)

if __name__=="__main__":
    design_path = r"/home/ruiz1/mongodb-d4/exps/tpcc_design"
    print design_path
    deserializer = Deserializer()
    deserializer.loadDesignFile(design_path)
    design = Design()
    design.data = deserializer.json_doc
    print design.data

    dm = DBDenormalizer(None, None, None, None, design)
    graph = dm.constructGraph()
    dm.metadata_db = pymongo.Connection('localhost:27017')['tpcc_meta']
    parent_keys = dm.readSchema('schema')

    combiner = DBCombiner(None, design, graph, parent_keys)

    operations = []
    for i in range(5):
        op = dict()
        op['query_content'] = [] 
        op['query_fields'] = None
        op['collection'] = 'order_line'
        op['query_content'].append({'ol_o_id':i,'ol_id':i+1})
        op['predicates'] = {'ol_o_id':'eq','ol_id':'eq'}
        operations.append(op)

    for i in range(3):
        op = dict()
        op['query_content'] = [] 
        op['query_fields'] = None
        op['collection'] = 'oorder'
        op['query_content'].append({'o_id':i})
        op['predicates'] = {'o_id':'eq'}
        operations.append(op)
        
 
    print "---Test combining deletes---"
    print "----------------------------"
    ret, error, updates = test_combine_deletes(combiner, operations)
    print ret
    print "----------------------------"
    print updates
    print "----------------------------"
