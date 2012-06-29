# -*- coding: utf-8 -*-

from datetime import datetime

import constants

## ==============================================
## DEFAULT CONFIGURATION
## ==============================================

SECT_MONGODB   = "mongodb"
SECT_CLUSTER   = "cluster"
SECT_DESIGNER  = "designer"
SECT_COSTMODEL = "costmodel"
SECT_MYSQL     = "mysql"

DEFAULT_CONFIG = {
    # MongoDB Configuration
    SECT_MONGODB: {
        "host": ("The hostname of the MongoDB instance to use for retrieving workload information", "localhost"),
        "port": ("The port number to the MongoDB instance", 27017),
        "metadata_db": ("The name of the database that the designer will use to store catalog information.", constants.METADATA_DB_NAME),
        "dataset_db": ("The name of the database that contains the sample data set", constants.DATASET_DB_NAME),
    },
    
    # Target Cluster Configuration
    SECT_CLUSTER: {
        "nodes": ("The number of machines in the target MongoDB cluster.", 10),
        "node_memory":  ("The amount of memory available for each MongoDB database nodes.", 15360),
        "page_size": ("The size of pages (kb) on disk for each MongoDB database node.", 4),
    },
    
    # Designer Configuration
    SECT_DESIGNER: {
        "enable_sharding": ("Enable the designer to look for sharding keys.", True),
        "enable_indexes":  ("Enable the designer to look for indexing keys.", True),
        "enable_denormalization":  ("Enable the designer to look for denormalization candidates.", True),
        "enable_local_search_increase": ("Enable increasing local search parameters after a restart", True),
        "sample_rate": ("Integer Percentage of dataset values to sample while gathering statistics.", 100),
    },
    
    # Cost Model Configuration
    SECT_COSTMODEL: {
        "weight_skew": ("Coefficient for the Skew cost function", 1.0),
        "weight_network": ("Coefficient for the Network cost function", 1.0),
        "weight_disk": ("Coefficient for the Disk cost function", 1.0),
        "time_intervals" : ("Number of intervals over which to examine the workload skew", constants.DEFAULT_TIME_INTERVALS),
        "address_size" : ("Size of an address for an index node in bytes", constants.DEFAULT_ADDRESS_SIZE),
    },
    
    # MySQL Conversion Configuration
    SECT_MYSQL: {
        "host":    ("MySQL host name", "localhost"),
        "port":    ("MySQL port number", 3306),
        "name":    ("The name of the MySQL database containing the sample data set", None),
        "user":    ("MySQL user name", None),
        "pass" :   ("MySQL user password", None),
    },
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
    
