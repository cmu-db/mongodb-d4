# -*- coding: utf-8 -*-

from datetime import datetime

import constants

## ==============================================
## DEFAULT CONFIGURATION
## ==============================================

KEY = "designer"

CONFIG = {
    "hostname": ("The hostname of the MongoDB server to use for retrieving workload information", "localhost"),
    "port":     ("The port number to the MongoDB", 27017 ),
    "schema_db": ("The name of the database that the designer will use to store catalog information.", "catalog"),
    "dataset_db": ("The name of the database that contains the sample data set", ""),
    "workload_db": ("The name of the database that contains the sample workload", "workload"),
}

def makeDefaultConfig():
    """Return a formatted version of the config dict that can be used with the --print-config command line argument"""
    ret =  "# %s Configuration File\n" % constants.PROJECT_NAME
    ret += "# Created %s\n" % (datetime.now())
    ret += "[%s]" % KEY
    
    max_len = max(map(lambda x: len(x), CONFIG.keys()))
    line_f = "\n\n# %s\n%-" + str(max_len) + "s = %s"
    for name in sorted(CONFIG.keys()):
        desc, default = CONFIG[name]
        if default == None: default = ""
        ret += line_f % (desc, name, default) 
    return (ret)
## DEF

def setDefaultValues(config):
    for key in CONFIG.keys():
        if not key in config:
            config[key] = CONFIG[key][-1]
    return (config)
## DEF
    
