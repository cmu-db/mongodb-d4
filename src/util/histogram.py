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

# TODO: Extend UserDict
class Histogram(dict):
    def __init__(self, *args, **kw):
        super(Histogram, self).__init__(*args, **kw)
        pass
    # DEF
    def put(self, x, delta=1):
        self[x] = self.get(x, 0) + delta
    # DEF
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
        ret = ""
        ret += "# of Elements: %d\n" % len(self)
        ret += "# of Samples:  %d\n" % sum(self.itervalues())
        ret += "="*50 + "\n"
        ret += "\n".join([ "%-25s %d" % (x, y) for x,y in self.iteritems() ])
        ret += "\n" + "="*50
        return (ret)
## CLASS