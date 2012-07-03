# -*- coding: utf-8 -*-

import os, sys

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))

import unittest

import workload
from util import constants

class TestUtilMethods(unittest.TestCase):
    
    def testIsOpRegex(self):
        op = {
            'collection': 'exfm.site.songs',
            'predicates': {'_id': constants.PRED_TYPE_REGEX},
            'query_aggregate': True,
            'query_content': [
                    {'#query': {'_id': {'#options': 'XXXXXXX',
                                        '#regex':   'YYYYY'}},
                     'count': 'site.songs',
                     'fields': None}],
           'query_group': None,
           'query_hash': 3563430808431869716L,
           'query_id': 579750519L,
           'query_limit': -1,
           'query_offset': 0,
           'query_size': 125,
           'query_time': 1338410992.894204,
           'resp_content': [{'n': 16, 'ok': 1}],
           'resp_id': 108641633L,
           'resp_size': 64,
           'resp_time': 1338410992.911907,
           'type': constants.OP_TYPE_QUERY,
           'update_multi': None,
           'update_upsert': None
        }

        ret = workload.isOpRegex(op)
        self.assertTrue(ret)

    ## DEF

    
## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN