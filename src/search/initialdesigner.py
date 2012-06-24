# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011 by Brown University
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

import design
from designer import *

## ==============================================
## InitialDesigner
## ==============================================
class InitialDesigner(Designer):
    
    def __init__(self, collections, statistics):
        self.collections = collections
        self.statistics = statistics
        print self
        print dir(self)
    
    ## DEF
    
    def generate(self):
        starting_design = design.Design()
        results = {}
        for col in self.collections :
            starting_design.addCollection(col['name'])
            results[col['name']] = {}
            col_fields = []
            for field, data in col['fields'].iteritems() :
                col_fields.append(field)
                results[col['name']][field] = self.calc_stats(params, statistics[col['name']]['fields'][field])
            attr = None
            value = 0
            for field, data in results[col['name']].iteritems() :
                if data >= value :
                    value = data
                    attr = field
            starting_design.addShardKey(col['name'], [attr])
            starting_design.addIndex(col['name'], [attr])
            
        return (starting_design)
    
    def calc_stats(params, stats):
        output = 0.0
        for k,v in params.iteritems():
            output += v * stats[k]
        return output
    ## DEF 
    
    
## CLASS