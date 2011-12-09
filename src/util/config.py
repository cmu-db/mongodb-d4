# -*- coding: utf-8 -*-

from datetime import datetime

import constants

## ==============================================
## DEFAULT CONFIGURATION
## ==============================================

SECT_MONGODB = "mongodb"
SECT_DESIGNER = "designer"
SECT_COSTMODEL = "costmodel"

DEFAULT_CONFIG = {
    # MongoDB Configuration
    SECT_MONGODB: {
        "hostname": ("The hostname of the MongoDB server to use for retrieving workload information", "localhost"),
        "port":     ("The port number to the MongoDB", 27017 ),
        "schema_db": ("The name of the database that the designer will use to store catalog information.", "catalog"),
        "dataset_db": ("The name of the database that contains the sample data set", ""),
        "workload_db": ("The name of the database that contains the sample workload", "designer"),
    },
    
    # Designer Configuration
    SECT_DESIGNER: {
        "enable_sharding": ("Enable the designer to look for sharding keys.", True),
        "enable_indexes":  ("Enable the designer to look for indexing keys.", False),
        "enable_denormalization":  ("Enable the designer to look for denormalization candidates.", False),
        
        "enable_local_search_increase": ("Enable increasing local search parameters after a restart", True),
    },
    
    # Cost Model Configuration
    SECT_COSTMODEL: {
        "weight_execution": ("", 1.0),
        "weight_skew": ("", 1.0),
    }
}

def makeDefaultConfig():
    """Return a formatted version of the config dict that can be used with the --print-config command line argument"""
    ret =  "# %s Configuration File\n" % constants.PROJECT_NAME
    ret += "# Created %s\n" % (datetime.now())
    
    first = True
    for key in DEFAULT_CONFIG.keys():
        if not first:
            ret += "\n\n# " + ("-"*60) + "\n"
        ret += "\n[%s]" % key
        max_len = max(map(lambda x: len(x), DEFAULT_CONFIG[key].keys()))
        line_f = "\n\n# %s\n%-" + str(max_len) + "s = %s"
        for name in sorted(DEFAULT_CONFIG[key].keys()):
            desc, default = DEFAULT_CONFIG[key][name]
            if default == None: default = ""
            ret += line_f % (desc, name, default) 
        ## FOR
        first = False
    ## FOR
        
    return (ret)
## DEF

def setDefaultValues(cparser):
    """Set the default values for the given SafeConfigParser"""
    for section in cparser.sections():
        assert section in DEFAULT_CONFIG, "Unexpected configuration section '%s'" % section
        for option in DEFAULT_CONFIG[section]:
            if not cparser.has_option(section, option):
                cparser.set(section, option, DEFAULT_CONFIG[key][-1])
        ## FOR
    ## FOR
    return (cparser)
## DEF
    
