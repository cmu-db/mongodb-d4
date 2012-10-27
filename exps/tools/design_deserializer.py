
import sys
import json
import os

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src/search"))

from design import Design

class Deserializer:
    def __init__(self, json_string):
        self.json_doc = json.loads(json_string)
    ## DEF

    def Deserialize(self):
        d = Design()
        self.__deserialize__(self.json_doc, d)
        return d
    ## DEF
    
    def __deserialize__(self, doc, design):
        """
            Just populate the given data into a design instance
        """
        for key, value in doc.iteritems():
            design.addCollection(key)
            for index in value['indexes']:
                design.addIndex(key, index)
            design.addShardKey(key, value['shardKeys'])
            design.setDenormalizationParent(key, value['denorm'])
        ## FOR
        
## CLASS
