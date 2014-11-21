import os
import sys
import logging
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

from search import bbsearch
LOG = logging.getLogger(__name__)

class TestShardKeyIterator(unittest.TestCase):
    def setUp(self):
        pass

    def testIfGeneratedAllCombination(self):
        expected = [["3", "2", "1"], ["3", "2"], ["3", "1"], ["2", "1"], ["3"], ["2"], ["1"]]
        iterator = bbsearch.ShardKeyIterator(["3", "2", "1"], -1)
        for combinations in expected:
            result = iterator.next()
            self.assertEqual(tuple(combinations), tuple(result))
            if len(result) == 1 and result[0] == "1":
                break

    def testIfGeneratedLimitedCombination(self):
        expected = [["3", "2"], ["3"], ["2"]]
        iterator = bbsearch.ShardKeyIterator(["3", "2", "1"], 2)
        for combinations in expected:
            result = iterator.next()
            self.assertEqual(tuple(combinations), tuple(result))
            if len(result) == 0 and result[0] == "2":
                break

if __name__ == '__main__':
    unittest.main()
