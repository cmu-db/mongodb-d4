# -*- coding: utf-8 -*-

import json
import logging

import design

import os
import sys

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../"))

from util import constants

LOG = logging.getLogger(__name__)

def fromJSON(input) :
    '''
    Convert the result of designer.py into a tuple of Design instances (initial, final)
    '''
    solutions = json.loads(input)
    initial = fromLIST(solutions['initial'])
    final = fromLIST(solutions['final'])
    return (initial, final)
    
def fromLIST(list) :
    d = design.Design()
    for col in list :
        d.addCollection(col['collection'])
        d.addShardKey(col['collection'], col['shardKey'])
        for i in col['indexes'] :
            d.addIndex(col['collection'], i)
        d.denorm[col['collection']] = col['denorm']
    return d

def getIndexSize(col_info, indexKeys):
        """Estimate the amount of memory required by the indexes of a given design"""
        # TODO: This should be precomputed ahead of time. No need to do this
        #       over and over again.
        index_size = 0
        for f_name in indexKeys:
            f = col_info.getField(f_name)
            assert f, "Invalid index key '%s.%s'" % (col_info['name'], f_name)
            index_size += f['avg_size']
        index_size += constants.DEFAULT_ADDRESS_SIZE
        
        #LOG.debug("%s Index %s Memory: %d bytes", col_info['name'], repr(indexKeys), index_size)
        return index_size
      
def buildLoadingList(design):
    """Generate the ordered list of collections based on the order that we need to load them"""
    LOG.debug("Computing collection load order")
    
    # First split the list of collections between those that are normalized
    # and those are not
    loadOrder = [ ]
    denormalized = { }
    for collection in design.getCollections():
        # Examine the design and see whether this collection
        # is denormalized into another collection
        if not design.isDenormalized(collection):
            loadOrder.append(collection)
        else:
            # Now for the denormalized guys, get their hierarchy
            # so that we can figure out who should get loaded first
            denormalized[collection] = design.getDenormalizationHierarchy(collection)
            LOG.debug("'%s' Denormalization Hierarchy: %s" % (collection, denormalized[collection]))
    ## FOR
    
    while len(denormalized) > 0:
        # Loop through each denormalized collection and remove any collection 
        # from their heirarchy that is already in the load list
        for collection in denormalized.keys():
            denormalized[collection] = filter(lambda x: not x in loadOrder, denormalized[collection])
        ## FOR
        
        # Now any collection that is not waiting for any other collection
        # can be loaded!
        newLoads = [ ]
        for collection in denormalized.keys():
            if len(denormalized[collection]) == 0:
                newLoads.append(collection)
        ## FOR
        assert len(newLoads) > 0, "Loading deadlock due to denormalization!"
        
        for collection in newLoads:
            loadOrder.append(collection)
            del denormalized[collection]
        ## FOR
    ## WHILE
    
    return loadOrder
## DEF