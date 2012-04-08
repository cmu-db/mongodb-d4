# -*- coding: utf-8 -*-

from util import *

## ==============================================
## Design
## ==============================================
class Design(object):

    def __init__(self):
        self.collections = []
        self.fields = {}
        self.shardKeys = {}
        self.indexes = {}

    def addCollection(self, collection) :
        if collection not in self.collections :
            self.collections.append(collection)
        
    def addCollections(self, collections) :
        for collection in collections :
            self.addCollection(collection)

## CLASS