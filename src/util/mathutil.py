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

import math
import functools
import logging
from pprint import pformat
    
LOG = logging.getLogger(__name__)
    
def quartiles(N):
    debug = LOG.isEnabledFor(logging.DEBUG)
    
    # Calculate the median
    median = percentile(N, 0.50)
    if debug: LOG.debug("Median: %s" % median)
    
    # Split into two halves
    # Do not include the median into the halves, or the minimum and maximum
    lower = []
    upper = []
    isUpper = False
    for i in xrange(1, len(N)-1):
        if not isUpper and N[i] >= median:
            isUpper = True
        if isUpper:
            upper.append(N[i])
        else:
            lower.append(N[i])
    ## FOR
    
    if debug: LOG.debug("Lower Portion: %d [%s-%s]" % (len(lower), lower[0], lower[-1]))
    if debug: LOG.debug("Upper Portion: %d [%s-%s]" % (len(upper), upper[0], upper[-1]))
    
    # Return (lowerQuartile, upperQuartile)
    return (percentile(lower, 0.50), percentile(upper, 0.50))
## DEF
    
## Original: http://code.activestate.com/recipes/511478-finding-the-percentile-of-the-values/
def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1
## DEF

## Original: FROM: http://www.physics.rutgers.edu/~masud/computing/WPark_recipes_in_python.html
def stddev(x):
    n, mean, std = len(x), 0, 0
    for a in x:
        mean = mean + a
    mean /= float(n)
    for a in x:
        std = std + (a - mean)**2
    std = math.sqrt(std / float(n-1))
    return std