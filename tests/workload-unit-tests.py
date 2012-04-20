#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import workload

class TestWorkload (unittest.TestCase) :
    
    def setUp(self) :
        pass
    
    def testAddSession(self) :
        session = {}
        w = workload.Workload()
        w.addSession(session)
        self.assertEqual(w.sessions, [session])
    
    def testAddSessions(self) :
        sessions = [{}, {}]
        w = workload.Workload()
        w.addSessions(sessions)
        self.assertEqual(w.sessions, sessions)
    
    def testWorkloadFactory(self) :
        w = workload.Workload.testFactory()
        self.assertEqual(isinstance(w, workload.Workload), True)
## End Class

if __name__ == '__main__':
    unittest.main()
## END MAIN