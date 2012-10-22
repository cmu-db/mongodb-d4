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
        rng = random.Random()
        for col_info in self.collections.itervalues():
            design.addCollection(col_info['name'])

            col_fields = []
            for field, data in col_info['fields'].iteritems():
                col_fields.append(field)

            # Figure out which attribute has the highest value for
            # the params that we care about when choosing the best design
            attrs = [ ]
            chosen_field = None
            while chosen_field is None or str(chosen_field).startswith("#") or str(chosen_field).startswith("_"):
                chosen_field = random.choice(col_fields)
            attrs.append(chosen_field)
            print "field: ", chosen_field

            design.addShardKey(col_info['name'], attrs)
            design.addIndex(col_info['name'], attrs)

        return design
    ## DEF

## CLASS