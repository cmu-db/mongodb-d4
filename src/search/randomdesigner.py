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
import design
import random

LOG = logging.getLogger(__name__)

## ==============================================
## InitialDesigner
## ==============================================
class RandomDesigner():

    def __init__(self, collections):
        self.collections = collections
        ## DEF

    def generate(self):
        LOG.info("Computing initial design")

        starting_design = design.Design()

        for col_info in self.collections :
            starting_design.addCollection(col_info['name'])

            col_fields = []

            for field, data in col_info['fields'].iteritems() :
                col_fields.append(field)

            # Figure out which attribute has the highest value for
            # the params that we care about when choosing the best design
            attrs = [ ]
            while len(attrs) == 0:
                counter = 0
                random_num = random.randint(0, len(col_fields))

                for field in col_fields:
                    if  counter == random_num:
                        attrs.append(field)
                        break
                    else:
                        counter += 1

            starting_design.addShardKey(col_info['name'], attrs)
            starting_design.addIndex(col_info['name'], attrs)

        return starting_design
        ## DEF

## CLASS