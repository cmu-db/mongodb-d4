# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

'''
This class is used as an abstraction of the actual workload. It is nothing more than 
a list of objects, more specifically, instances of the SyntheticSession class. This 
approach should provide flexibility in adding new attributes in the future without 
having to change the data import/parsing processes.  
'''
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
    
    def factory(self) :
        return Workload()
# END CLASS