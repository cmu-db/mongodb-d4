
import unittest
import os
import sys

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../src/search"))
sys.path.append(os.path.join(basedir, "../"))

from util import configutil
from util import constants

from tpcctestcase import TPCCTestCase
from ConfigParser import RawConfigParser
from search.designer import Designer
from search import Design
from designcandidates import DesignCandidates
import itertools
from initialdesigner import InitialDesigner
from randomdesigner import RandomDesigner
from lnsdesigner import LNSDesigner
from randomdesigner import RandomDesigner
from costmodel import CostModel
from tpcc import constants as tpccConstants
from search import bbsearch

LNS_RUN_TIME = 36000 # seconds

class FindExpectedDesign(TPCCTestCase):
    """
        Try to see if the existing cost model could generate the best desgin we
        expected
    """
    def setUp(self):
        TPCCTestCase.setUp(self)

        config = RawConfigParser()
        configutil.setDefaultValues(config)
        config.read(os.path.realpath('./exfm.config'))

        self.designer = Designer(config, self.metadata_db, self.dataset_db)
        self.dc = self.designer.generateDesignCandidates(self.collections)
        self.assertIsNotNone(self.dc)
        
        # Make sure that we don't have any invalid candidate keys
        for col_name in self.collections.iterkeys():
            for index_keys in self.dc.indexKeys[col_name]:
                for key in index_keys:
                    assert not key.startswith(constants.REPLACE_KEY_DOLLAR_PREFIX), \
                        "Unexpected candidate key '%s.%s'" % (col_name, key)
        ## FOR
        
    ## DEF


    def outtestfindExpectedDesign(self):
        """Perform the actual search for a design"""
        # Generate all the design candidates
        # Instantiate cost model
        cmConfig = {
            'weight_network': 4,
            'weight_disk':    1,
            'weight_skew':    1,
            'nodes':          10,
            'max_memory':     1024,
            'skew_intervals': 10,
            'address_size':   64,
            'window_size':    500
        }
        cm = CostModel(self.collections, self.workload, cmConfig)
#        if self.debug:
#            state.debug = True
#            costmodel.LOG.setLevel(logging.DEBUG)

        # Compute initial solution and calculate its cost
        # This will be the upper bound from starting design

#        initialDesign = InitialDesigner(self.collections, self.workload, None).generate()
        initialDesign = RandomDesigner(self.collections, self.workload, None).generate()
        upper_bound = cm.overallCost(initialDesign)
        if upper_bound < 0.5:
            exit()
        collectionNames = [c for c in self.collections]
        
        dc = self.dc.getCandidates(collectionNames)
        
        d = Design()
        for col_name in collectionNames:
            d.addCollection(col_name)
            d.reset(col_name)
        print "empty design: ", d
        print "design candidates: ", dc
        
        ln = LNSDesigner(self.collections, \
                        self.dc, \
                        self.workload, \
                        None, \
                        cm, \
                        initialDesign, \
                        upper_bound, \
                        LNS_RUN_TIME)
        solution = ln.solve()
        print "solution: ", solution
        print "inii solution: ", initialDesign
        print "init solution cost: ", upper_bound
        
    def getManMadeBestDesign(self, denorm=True):
       # create a best design mannually
       
        d = Design()
        d.addCollection(tpccConstants.TABLENAME_ITEM)        
        d.addCollection(tpccConstants.TABLENAME_WAREHOUSE)
        d.addCollection(tpccConstants.TABLENAME_DISTRICT)
        d.addCollection(tpccConstants.TABLENAME_CUSTOMER)
        d.addCollection(tpccConstants.TABLENAME_STOCK)
        d.addCollection(tpccConstants.TABLENAME_ORDERS)
        d.addCollection(tpccConstants.TABLENAME_NEW_ORDER)
        d.addCollection(tpccConstants.TABLENAME_ORDER_LINE)
        
        d.addIndex(tpccConstants.TABLENAME_ITEM, ["I_ID"])
        d.addIndex(tpccConstants.TABLENAME_WAREHOUSE, ["W_ID"])
        d.addIndex(tpccConstants.TABLENAME_DISTRICT, ["D_W_ID", "D_ID"])
        d.addIndex(tpccConstants.TABLENAME_CUSTOMER, ["C_W_ID", "C_D_ID","C_ID"])
        d.addIndex(tpccConstants.TABLENAME_ORDERS, ["O_W_ID", "O_D_ID", "O_C_ID"])
        d.addIndex(tpccConstants.TABLENAME_ORDERS, ["O_W_ID", "O_D_ID", "O_ID"])
        d.addIndex(tpccConstants.TABLENAME_STOCK, ["S_W_ID", "S_I_ID"])
        d.addIndex(tpccConstants.TABLENAME_NEW_ORDER, ["NO_W_ID", "NO_D_ID", "NO_O_ID"])
        d.addIndex(tpccConstants.TABLENAME_ORDER_LINE, ["OL_W_ID", "OL_D_ID", "OL_O_ID"])
        
        d.addShardKey(tpccConstants.TABLENAME_ITEM, ["I_ID"])
        d.addShardKey(tpccConstants.TABLENAME_WAREHOUSE, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_DISTRICT, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_CUSTOMER, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_ORDERS, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_STOCK, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_NEW_ORDER, ["W_ID"])
        d.addShardKey(tpccConstants.TABLENAME_ORDER_LINE, ["W_ID"])
        
        if denorm:
            d.setDenormalizationParent(tpccConstants.TABLENAME_ORDER_LINE, tpccConstants.TABLENAME_ORDERS)
        
        return d
    
if __name__ == '__main__':
    unittest.main()
## MAIN
