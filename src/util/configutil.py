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

ALL_SECTIONS = set()
_key = None
for _key in locals():
    if _key.startswith("SECT_"): ALL_SECTIONS.add(locals()[_key])

# FORMAT: (<name>, <description>, <default-value>)
DEFAULT_CONFIG = {
    # MongoDB Configuration
    SECT_MONGODB: [
        ("host", "The hostname of the MongoDB instance to use for retrieving workload information", "localhost"),
        ("port", "The port number to the MongoDB instance", 27017),
        ("metadata_db", "The name of the database that the designer will use to store catalog information.",
constants.METADATA_DB_NAME),
        ("dataset_db", "The name of the database that contains the sample data set", constants.DATASET_DB_NAME),
    ],
    
    # Target Cluster Configuration
    SECT_CLUSTER: [
        ("nodes", "The number of machines in the target MongoDB cluster.", 10),
        ("node_memory", "The amount of memory available (MB) for each MongoDB database nodes.", 15360),
    ],
    
    # Designer Configuration
    SECT_DESIGNER: [
        ("enable_sharding", "Enable the designer to look for sharding keys.", True),
        ("enable_indexes", "Enable the designer to look for indexing keys.", True),
        ("enable_denormalization", "Enable the designer to look for denormalization candidates.", True),
        ("enable_local_search_inc", "Enable increasing local search parameters after a restart", True),
        ("sample_rate", "Integer Percentage of dataset values to sample while gathering statistics.", 100),
    ],
    
    # Cost Model Configuration
    SECT_COSTMODEL: [
        ("weight_skew", "Coefficient for the Skew cost function", 1.0),
        ("weight_network", "Coefficient for the Network cost function", 1.0),
        ("weight_disk", "Coefficient for the Disk cost function", 1.0),
        ("time_intervals", "Number of intervals over which to examine the workload skew",
constants.DEFAULT_TIME_INTERVALS),
        ("address_size", "Size of an address for an index node in bytes", constants.DEFAULT_ADDRESS_SIZE),
    ],
    
    # MySQL Conversion Configuration
    SECT_MYSQL: [
        ("host", "MySQL host name", "localhost"),
        ("port", "MySQL port number", 3306),
        ("name", "The name of the MySQL database containing the sample data set", None),
        ("user", "MySQL user name", None),
        ("pass", "MySQL user password", None),
    ],
}

## ==============================================
## formatDefaultConfig
## ==============================================
def formatDefaultConfig():
    """Return a formatted version of the config dict that can be used with the --print-config command line argument"""
    ret =  "# %s Configuration File\n" % constants.PROJECT_NAME
    ret += "# Created %s\n" % (datetime.now())
    
    first = True
    for key in ALL_SECTIONS:
        if not first:
            ret += "\n\n# " + ("-"*60) + "\n"
        ret += "\n[%s]" % key
        max_len = max(map(lambda x: len(x[0]), DEFAULT_CONFIG[key]))
        line_f = "\n\n# %s\n%-" + str(max_len) + "s = %s"
        for name, desc, default in DEFAULT_CONFIG[key]:
            if default == None: default = ""
            ret += line_f % (desc, name, default) 
        ## FOR
        first = False
    ## FOR
        
    return (ret)
## DEF

## ==============================================
## formatConfigList
## ==============================================
def formatConfigList(name, config):
    """
        Return a formatted version of the config list that can be used with the --config command line argument.
    """

    # Header
    ret = "\n# " + ("="*75) + "\n"
    ret += "[%s]" % name
    
    # Benchmark Configuration
    for key, desc, default in config:
        if default == None: default = ""
        ret += "\n\n# %s\n%-20s = %s" % (desc, key, default) 
    ret += "\n"
    return (ret)
## DEF

## ==============================================
## makeDefaultConfig
## ==============================================
def makeDefaultConfig():
    """
        Return a ConfigParser of the default configuration
    """
    from ConfigParser import RawConfigParser
    return setDefaultValues(RawConfigParser())
## DEF

## ==============================================
## setDefaultValues
## ==============================================
def setDefaultValues(config):
    """Set the default values for the given SafeConfigParser"""
    for sect in ALL_SECTIONS:
        if not config.has_section(sect):
            config.add_section(sect)
        for key, desc, default in DEFAULT_CONFIG[sect]:
            if not config.has_option(sect, key):
                config.set(sect, key, default)
                    
        ## FOR
    ## FOR
    return (config)
## DEF
    
