# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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
import math

class Histogram(dict):
    def __init__(self, *args, **kw):
        super(Histogram, self).__init__(*args, **kw)
        
        self.min_keys = None
        self.min_cnt = None
        self.max_keys = None
        self.max_cnt = None
        
        pass
    # DEF
    def put(self, x, delta=1):
        self[x] = self.get(x, 0) + delta
    # DEF
    def remove(self, x, delta=1):
        self[x] = self.get(x, 0) - delta
    # DEF
    
    def __computeInternalValues__(self):
        self.min_keys = [ ]
        self.min_cnt = None
        self.max_keys = [ ]
        self.max_cnt = None
        
        for key, cnt in self.iteritems():
            if not self.min_cnt or cnt <= self.min_cnt:
                if cnt < self.min_cnt: self.min_keys = []
                self.min_keys.append(key)
                self.min_cnt = cnt
            if not self.max_cnt or cnt >= self.max_cnt:
                if cnt > self.max_cnt: self.max_keys = []
                self.max_keys.append(key)
                self.max_cnt = cnt
        ## FOR
    ## DEF
    
    def getSampleCount(self):
        """Return the number of samples added to this histogram"""
        return sum(self.itervalues())
    ## DEF
    
    def getValuesForCount(self, count):
        """Return all the keys that have the same count as the given parameter"""
        keys = [ ]
        for key, cnt in self.iteritems():
            if count == cnt: keys.append(key)
        return keys
    ## DEF
    
    def getCounts(self):
        """Return all the unique count values in the histogram"""
        return set(self.itervalues())
    ## DEF
    
    def getCountMean(self):
        """Return the mean for the counts in the histograms"""
        return sum(self.itervalues()) / float(len(self))
    ## DEF
    
    def getMinCountKeys(self):
        self.__computeInternalValues__()
        return self.min_keys
    ## DEF
    def getMaxCountKeys(self):
        self.__computeInternalValues__()
        return self.max_keys
    ## DEF
    
    def toJava(self):
        output = ""
        for key in sorted(self.iterkeys()):
            cnt = self[key]
            if type(key) == str:
                key = "\"%s\"" % (key.replace('"', '\\"'))
            output += "this.put(%s, %d);\n" % (key, cnt)
        return output
    ## DEF

    def __str__(self):

        self.__computeInternalValues__()

        ret = ""
#        ret += "# of Elements: %d\n" % len(self)
#        ret += "# of Samples:  %d\n" % self.getSampleCount()

        max_value_length = 20
        max_bar_length = 80
        marker = "*"

        # Figure out the max size of the counts
        max_ctr_length = 4
        total = 0
        for ctr in self.itervalues():
            total += ctr
            max_ctr_length = max(max_ctr_length, len(str(ctr)))
        ## FOR

        # Don't let anything go longer than MAX_VALUE_LENGTH chars
        f = "%-" + str(max_value_length) + "s [%" + str(max_ctr_length) + "d] "

        first = True
        for value in sorted(self.iterkeys()):
            if not first: ret += "\n"
            value_str = str(value)
            if len(value_str) > max_value_length:
                value_str = value_str[:(max_value_length - 3)] + "..."

            # Value Label + Count
            cnt = self[value]
            ret += f % (value_str, cnt)

            # Histogram Bar
            barSize = int(cnt / float(self.max_cnt)) * max_bar_length
            ret += marker*barSize

            first = False
        ## FOR
        if not len(self): ret += "<EMPTY>"

        return ret
## CLASS