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
import re
import subprocess

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise Exception("ERROR: Missing database name")

    db_name = sys.argv[1]
    cmd = "mongo %s --eval 'db.getCollectionNames()'" % db_name
    output = subprocess.check_output(cmd, shell=True)

    collections = set()
    for line in output.strip().split("\n"):
        if line.find("system.indexes") != -1:
            map(collections.add, line.split(","))
    collections.remove("system.indexes")

    os.mkdir(db_name)
    for c in collections:
        output = os.path.join(db_name, "%s.json" % c)
        cmd = "mongoexport --db %s --collection %s --out %s" % (db_name, c, output)
        subprocess.check_call(cmd, shell=True)
        print output
## IF