#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging
from ConfigParser import SafeConfigParser
from pymongo import Connection

from util import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description=constants.PROJECT_NAME)
    aparser.add_argument('--config', type=file,
                         help='Path to designer configuration file')
    aparser.add_argument('--host', type=str, default="localhost",
                         help='The hostname of the MongoDB instance containing the sample workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file used by the MongoDB-Designer')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    if args['print_config']:
        print config.makeDefaultConfig()
        print
        sys.exit(0)
    
    logging.debug("Loading configuration file '%s'" % args['config'])
    cparser = SafeConfigParser()
    cparser.read(os.path.realpath(args['config'].name))
    config = dict(cparser.items(config.KEY))
    
    ## ----------------------------------------------
    ## STEP 1
    ## Precompute any summarizations and information that we can about the workload
    ## ----------------------------------------------
    
    ## ----------------------------------------------
    ## STEP 2
    ## Generate an initial solution
    ## ----------------------------------------------
    
    ## ----------------------------------------------
    ## STEP 3
    ## Execute the LNS design algorithm
    ## ----------------------------------------------
    
## MAIN