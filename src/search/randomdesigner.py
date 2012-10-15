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

import logging
import random

# mongodb-d4
from design import Design
from abstractdesigner import AbstractDesigner

LOG = logging.getLogger(__name__)

## ==============================================
## InitialDesigner
## ==============================================
class RandomDesigner(AbstractDesigner):

    def __init__(self, collections, workload, config):
        AbstractDesigner.__init__(self, collections, workload, config)
    ## DEF

    def generate(self):
        LOG.info("Generating random design")
        design = Design()
        for col_info in self.collections.itervalues():
            design.addCollection(col_info['name'])

            col_fields = []
            for field, data in col_info['fields'].iteritems():
                col_fields.append(field)

            # Figure out which attribute has the highest value for
            # the params that we care about when choosing the best design
            attrs = [ ]
            while len(attrs) == 0:
                counter = 0
                random_num = random.randint(0, len(col_fields))

                for field in col_fields:
                    if  counter == random_num:
                        if str(field).startswith("#") or str(field).startswith("_"):
                            break
                        attrs.append(field)
                        break
                    else:
                        counter += 1

            design.addShardKey(col_info['name'], attrs)
            design.addIndex(col_info['name'], attrs)

        return design
    ## DEF

## CLASS