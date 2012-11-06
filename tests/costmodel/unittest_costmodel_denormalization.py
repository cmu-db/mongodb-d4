
import unittest
import os
import sys

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../src/search"))
sys.path.append(os.path.join(basedir, "../"))

from util import constants
from tpcctestcase import TPCCTestCase
from search import Design
from costmodel import CostModel
from tpcc import constants as tpccConstants

class FindExpectedDesign(TPCCTestCase):
    """
        Try to see if the existing cost model could generate the best desgin we
        expected
    """
    def setUp(self):
        TPCCTestCase.setUp(self)
    ## DEF

    def testfindExpectedDesign(self):
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
        d0 = self.getManMadeDesign()
        cost0 = cm.overallCost(d0)

        d1 = d0.copy()
        d1.setDenormalizationParent(tpccConstants.TABLENAME_ORDER_LINE, tpccConstants.TABLENAME_ORDERS)
        cost1 = cm.overallCost(d1)

        self.assertLess(cost1, cost0)
    ## def

    def getManMadeDesign(self, denorm=True):
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

        return d

if __name__ == '__main__':
    unittest.main()
## MAIN
