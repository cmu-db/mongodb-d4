
import unittest
import os
import sys
import time

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../src/search"))

from util import configutil
from tpcctestcase import TPCCTestCase
from ConfigParser import RawConfigParser
from search.designer import Designer
from designcandidates import DesignCandidates
import itertools
from initialdesigner import InitialDesigner
from lnsdesigner import LNSDesigner
from randomdesigner import RandomDesigner
from costmodel import CostModel

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

        isShardingEnabled = True
        isIndexesEnabled = True
        isDenormalizationEnabled = True

        shardKeys = []
        indexKeys = [[]]
        denorm = []
        self.dc = DesignCandidates()

        for col_info in self.collections.itervalues():
            interesting = col_info['interesting']

            # deal with shards
            if isShardingEnabled:
                shardKeys = interesting

            # deal with indexes
            if isIndexesEnabled:
                for o in xrange(1, len(interesting) + 1) :
                    for i in itertools.combinations(interesting, o) :
                        indexKeys.append(i)

            # deal with de-normalization
            if isDenormalizationEnabled:
                for k,v in col_info['fields'].iteritems() :
                    if v['parent_col'] <> '' and v['parent_col'] not in denorm :
                        denorm.append(v['parent_col'])

            self.dc.addCollection(col_info['name'], indexKeys, shardKeys, denorm)
            ## FOR

    def testfindExpectedDesign(self):
        """Perform the actual search for a design"""
        # Generate all the design candidates
        # Instantiate cost model
        cmConfig = {
            'weight_network': 1,
            'weight_disk':    1,
            'weight_skew':    1,
            'nodes':          10,
            'max_memory':     1024,
            'skew_intervals': 10,
            'address_size':   64,
            'window_size':    1000
        }
        cm = CostModel(self.collections, self.workload, cmConfig)
#        if self.debug:
#            state.debug = True
#            costmodel.LOG.setLevel(logging.DEBUG)

        # Compute initial solution and calculate its cost
        # This will be the upper bound from starting design

        initialDesign = InitialDesigner(self.collections, self.workload, None).generate()
        
        upper_bound = cm.overallCost(initialDesign)
        print "initial Design cost: ", upper_bound
        ln = LNSDesigner(self.collections, self.dc, self.workload, None, cm, initialDesign, upper_bound, 12000)
        solution = ln.solve()

        print "solution: \n", solution


if __name__ == '__main__':
    unittest.main()
## MAIN
