#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import workload

class TestQuery (unittest.TestCase) :
    
    def setUp(self) :
        pass
    
    def testCollectionSetting(self) :
        query = workload.Query()
        query.collection = 'test'
        self.assertEqual(query.collection, 'test')
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN