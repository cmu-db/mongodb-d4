# -*- coding: utf-8 -*-

class Workload(object) :

    def __init__(self) :
        self.sessions = []
        
    def addSession(self, session) :
        self.sessions.append(session)
    
    def addSessions(self, sessions) :
        for session in sessions :
            self.addSession(session)
    
    @staticmethod
    def testFactory() :
        return Workload()

# END CLASS