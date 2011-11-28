# -*- coding: utf-8 -*-

from datetime import datetime

import constants

## ==============================================
## DEFAULT CONFIGURATION
## ==============================================

KEY = "designer"

CONFIG = {
    "hostname": ("The hostname of the MongoDB server to use for retrieving workload information", "localhost"),
    "workload_name": ("The name of the database that contains the same workload", "designer"),
    "workload_collection": ("The name of the collection that contains the sample workload", "mongo_comm"),
}

def makeDefaultConfig():
    """Return a formatted version of the config dict that can be used with the --print-config command line argument"""
    ret =  "# %s Configuration File\n" % constants.PROJECT_NAME
    ret += "# Created %s\n" % (datetime.now())
    ret += "[%s]" % KEY
    
    max_len = max(map(lambda x: len(x), CONFIG.keys()))
    line_f = "\n\n# %s\n%-" + str(max_len) + "s = %s"
    for name in CONFIG.keys():
        desc, default = CONFIG[name]
        if default == None: default = ""
        ret += line_f % (desc, name, default) 
    return (ret)
## DEF
