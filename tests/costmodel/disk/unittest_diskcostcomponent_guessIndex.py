#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pprint import pformat
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../src"))
sys.path.append(os.path.join(basedir, "../"))

# mongodb-d4
from costmodel.disk import DiskCostComponent
from search import Design
from workload import Session
from costmodel.state import State
from costmodeltestcase_guessIndex import CostModelTestCase

class TestDiskCostGuessIndex(CostModelTestCase):

    def setUp(self):
        CostModelTestCase.setUp(self)

    def testGuessIndex_consistentAnswer(self):
        """Check that guessIndex always returns the same answer for the same input"""
        cm = DiskCostComponent(self.state)
        ops = []
        for sess in self.workload:
            map(ops.append, sess["operations"])

        # initialize design
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field00", "field01"])
        d.addIndex("apple", ["field01", "field00"])
        d.addIndex("apple", ["field00"])
        d.addIndex("apple", ["field01"])

        for op in ops:
            last_index, last_covering = (None, None)
            for i in xrange(100):
                best_index, covering = cm.guessIndex(d, op)
                self.assertIsNotNone(best_index)
                self.assertIsNotNone(covering)
                if not last_index is None:
                    self.assertEqual(last_index, best_index)
                    self.assertEqual(last_covering, covering)
                last_index, last_covering = (best_index, covering)
            ## FOR
    ## DEF

    def testGuessIndex_indexInIncorrectOrder(self):
        """
            Design with index (field01, field00)
            1. query uses index (field00)
            result: not using index because that query uses indexes in order
            2. query uses index (field01)
            result: using index (field01, field00) because this is the best match
            3. query uses index (field01, field00)
            result: using index (field01, field00) because they match the best

            Design with index (field00, field01)
            4. query uses index (field01, field00)
            result: using no index because the index order is not correct

            Design with index (field01, field02, field00)
            5. query uses index (field01, field00)
            result: using index (field01, field02, field00) because they match the best
            result: not cover index because the index order in design is not correct
        """
        cm = DiskCostComponent(self.state)
        ops = []
        for sess in self.workload:
            for op in sess["operations"]:
                ops.append(op)

        # initialize design
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field01", "field00"])

        # query 1: get query, queries on field00
        op = ops[0]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(best_index, None)
        self.assertFalse(covering)

        # query 2: get query, queries on field01
        op = ops[1]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 2)
        self.assertEqual(best_index[0], "field01")
        self.assertEqual(best_index[1], "field00")
        self.assertFalse(covering)

        # query 3: get query, queries on field01 and field00
        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 2)
        self.assertEqual(best_index[0], "field01")
        self.assertEqual(best_index[1], "field00")
        self.assertFalse(covering)

        # query 4:
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field00", "field01"])

        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 2)
        self.assertFalse(covering)

        # query 5:
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field01", "field02", "field00"])

        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 3)
        self.assertEqual(best_index[0], "field01")
        self.assertEqual(best_index[1], "field02")
        self.assertEqual(best_index[2], "field00")
        self.assertFalse(covering)

    def testGuessIndex_indexChooseTheMostMatch(self):
        """
            Design with index (field01, field00), (field01),
            1. query uses index (field01) without projection field
            result: using index (field01) because they match the most
            2. query used index (field01, field00) without projection field
            result: using index (field01, field00) because they match the most

            If we have a design building indexes on (field01) only
            3. query uses index (field01, field00) without projection field
            result: using index (field01) because they match the most

            If we have a design building indexes on (field01, field03, field00), (field01)
            4. query uses index (field01, field00)
            result: using index (field01) because field01 is shorter
        """
        cm = DiskCostComponent(self.state)
        ops = []
        for sess in self.workload:
            for op in sess["operations"]:
                ops.append(op)

        # initialize design
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field01", "field00"])
        d.addIndex("apple", ["field01"])

        # query 1: get query
        op = ops[1]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 1)
        self.assertEqual(best_index[0], 'field01')
        self.assertFalse(covering)

        # query 2:  get query
        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 2)
        self.assertEqual(best_index[0], 'field01')
        self.assertEqual(best_index[1], 'field00')
        self.assertFalse(covering)

        ## query 3:
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field01"])

        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(best_index[0], 'field01')
        self.assertFalse(covering)

        # query 4:
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field01", "field03", "field00"])
        d.addIndex("apple", ["field01"])
        op = ops[2]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 1)
        self.assertEqual(best_index[0], 'field01')
        self.assertFalse(covering)

    def testGuessIndex_indexChooseWithProjectionField(self):
        """
            If a query uses one of the indexes the design has but its projection uses
            one of the indexes the design has, we should choose the index with both
            query index and projection index
        """
        # If we have a design with index (field00), (field00, field02)
        # 1. query uses field00 but its projection field is {field02: xx}
        # result: we should choose (field00, field02) as the best index
        cm = DiskCostComponent(self.state)
        ops = []
        for sess in self.workload:
            for op in sess["operations"]:
                ops.append(op)

        # initialize design
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field00", "field02"])
        d.addIndex("apple", ["field00"])

        op = ops[0]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(len(best_index), 2)
        self.assertEqual(best_index[0], "field00")
        self.assertEqual(best_index[1], "field02")
        self.assertTrue(covering)
    ## DEF

    def testGuessIndex_indexChooseWithoutProjectionField(self):
        """
            If a query uses all the indexes but doesn't have a projection field,
            we still think it is not a covering index
        """
        # If we have a design with indexes(field00, field01)
        # 1. query uses (field00, field01) but there is no projection field
        # result: we should choose (field00, field02) but the index is not a covering index
        cm = DiskCostComponent(self.state)
        ops = []
        for sess in self.workload:
            for op in sess["operations"]:
                ops.append(op)

        # initialize design
        d = Design()
        d.addCollection("apple")
        d.addIndex("apple", ["field00", "field01"])

        op = ops[3]

        # Guess index
        best_index, covering = cm.guessIndex(d, op)

        self.assertEqual(best_index[0], "field00")
        self.assertEqual(best_index[1], "field01")
        self.assertFalse(covering)
    ## DEF
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN
