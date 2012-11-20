#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import os, sys
import subprocess
import logging
import glob
from ConfigParser import RawConfigParser

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../libs"))
import argparse

from util import constants
from util import configutil

logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S",
    stream = sys.stdout
)
LOG = logging.getLogger(__name__)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description="CSV File Loader")
    aparser.add_argument('input', help='CSV Input Data Dump Directory')
    aparser.add_argument('--config', type=file, help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--debug', action='store_true', help='Enable debug log messages.')
    args = vars(aparser.parse_args())
    if args['debug']: LOG.setLevel(logging.DEBUG)
    
    if not args['config']:
        LOG.error("Missing configuration file")
        print
        aparser.print_usage()
        sys.exit(1)
    LOG.debug("Loading configuration file '%s'" % args['config'])
    config = RawConfigParser()
    configutil.setDefaultValues(config)
    config.read(os.path.realpath(args['config'].name))
    
    db_host = config.get(configutil.SECT_MONGODB, 'host')
    db_name = config.get(configutil.SECT_MONGODB, 'dataset_db')
    for dataFile in glob.glob(os.path.join(args["input"], "*.json")):
        collection = os.path.basename(dataFile).replace(".csv", "")
        cmd = "mongoimport --host=%s --db %s --collection %s --file %s --type json" % (db_host, db_name, collection, dataFile)
        subprocess.check_call(cmd, shell=True)
        LOG.info("Loaded %s.%s", db_name, collection)
    ## FOR
## IF