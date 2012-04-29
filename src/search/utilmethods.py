# -*- coding: utf-8 -*-

import json
import logging

import design

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