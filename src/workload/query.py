# -*- coding: utf-8 -*-

class Query(object) :

    def __init__(self) :
        self.collection = None
        self.type = None
        self.predicates = {}
        self.timestamp = None
        self.projection = {}
        
# END CLASS