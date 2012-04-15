# -*- coding: utf-8 -*-

class Workload(object) :

    def __init__(self) :
        self.sessions = []
        
    def addSession(self, session) :
        self.sessions.append(session)
    
    def addSessions(self, sessions) :
        for session in sessions :
            self.addSession(session)
    
    @property
    def length(self) :
        return len(self.sessions)
        
    @staticmethod
    def testFactory() :
        return Workload()
    
    @staticmethod
    def factory() :
        return Workload()
# END CLASS