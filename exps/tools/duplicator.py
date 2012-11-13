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
from __future__ import division
from __future__ import with_statement

import os, sys
import argparse
import logging
import random
import re
import string
import json
import glob
import codecs
from pprint import pformat
from ConfigParser import RawConfigParser

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../src"))
sys.path.append(os.path.join(basedir, "../../libs"))
import mongokit

# mongodb-d4
import catalog
import workload
from search import Designer
from util import configutil
from util import constants
from util.histogram import Histogram

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

LOG = logging.getLogger(__name__)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description="CSV File Duplicator")
    aparser.add_argument('input', help='CSV Input Data Dump Directory')
    aparser.add_argument('output', help='CSV Output Data Dump Directory')
    aparser.add_argument('multiplier', type=int, help='Data Duplicator Multiplier')
    aparser.add_argument('--debug', action='store_true', help='Enable debug log messages.')
    args = vars(aparser.parse_args())
    if args['debug']: LOG.setLevel(logging.DEBUG)
    
    if not os.path.exists(args["output"]):
        os.mkdir(args["output"])
    for dataFile in glob.glob(os.path.join(args["input"], "*.json")):
        newDataFile = os.path.join(args["output"], os.path.basename(dataFile))
        with codecs.open(newDataFile, encoding='utf-8', mode='w+') as out:
            with codecs.open(dataFile, encoding='utf-8') as fd:
                new_ctr = 0
                orig_ctr = 0
                for line in fd:
                    try:
                        row = json.loads(line.encode('utf-8'))
                    except:
                        LOG.error(row)
                        raise
                    id = row["_id"]["$oid"]
                    out.write(line)
                    orig_ctr += 1
                    new_ctr += 1
                    for i in xrange(args['multiplier']-1):
                        # Just update the _id field
                        new_id = str(int(id[0])+1) + id[1:]
                        out.write(line.replace(id, new_id))
                        new_ctr += 1
                    ## FOR
                ## FOR
            ## WITH
            LOG.info("DUPLICATED %s -> ORIG:%d / NEW:%d", newDataFile, orig_ctr, new_ctr)
        ## WITH
    ## FOR


## MAIN
