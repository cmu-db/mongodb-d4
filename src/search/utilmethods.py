# -*- coding: utf-8 -*-

import json
import design

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
    