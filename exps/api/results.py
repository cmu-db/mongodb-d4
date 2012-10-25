# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
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

import logging
import time

from util.histogram import Histogram

LOG = logging.getLogger(__name__)

class Results:
    
    def __init__(self):
        self.start = None
        self.stop = None
        self.txn_id = 0
        self.opCount = 0
        self.completed = [ ] # (txnName, timestamp)
        self.txn_counters = Histogram()
        self.txn_times = { }
        self.running = { }
        
    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start == None
        LOG.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start
        
    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        LOG.debug("Stopping benchmark statistics collection")
        self.stop = time.time()
        
    def startTransaction(self, txn):
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time())
        return id
        
    def abortTransaction(self, id):
        """Abort a transaction and discard its times"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]
        
    def stopTransaction(self, id, opCount):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        
        timestamp = time.time()
        
        txn_name, txn_start = self.running[id]
        del self.running[id]
        self.completed.append((txn_name, timestamp))
        
        duration = timestamp - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration
        
        # OpCount
        self.opCount += opCount
        
        # Txn Counter Histogram
        self.txn_counters.put(txn_name)
        assert self.txn_counters[txn_name] > 0
        
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Completed %s in %f sec" % (txn_name, duration))
    ## DEF
        
    def append(self, r):  
        self.opCount += r.opCount
        for txn_name in r.txn_counters.keys():
            self.txn_counters.put(txn_name, delta=r.txn_counters[txn_name])
            
            orig_time = self.txn_times.get(txn_name, 0)
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            
            #LOG.info("resOps="+str(r.opCount))
            #LOG.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
        ## HACK
        if type(r.completed) == list:
            self.completed.extend(r.completed)
        if not self.start:
            self.start = r.start
        else:
            self.start = min(self.start, r.start)
        if not self.stop:
            self.stop = r.stop
        else:
            self.stop = max(self.stop, r.stop)
    ## DEF
            
    def __str__(self):
        return self.show()
        
    def show(self, load_time = None):
        if self.start == None:
            msg = "Attempting to get benchmark results before it was started"
            raise Exception(msg)
            LOG.warn(msg)
            return "Benchmark not started"
        if self.stop == None:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start
        
        col_width = 18
        total_width = (col_width*4)+2
        f = "\n  " + (("%-" + str(col_width) + "s")*4)
        line = "-"*total_width

        ret = u"" + "="*total_width + "\n"
        if load_time != None:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)
        
        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Executed", u"Total Time (ms)", "Rate") 
        total_time = duration
        total_cnt = self.txn_counters.getSampleCount()
        #total_running_time = 0
        
        for txn in sorted(self.txn_counters.keys()):
            txn_time = self.txn_times[txn]
            txn_cnt = "%6d - %4.1f%%" % (self.txn_counters[txn], (self.txn_counters[txn] / float(total_cnt))*100)
            rate = u"%.02f txn/s" % ((self.txn_counters[txn] / total_time))
            #total_running_time +=txn_time
            #rate = u"%.02f op/s" % ((self.txn_counters[txn] / total_time))
            #rate = u"%.02f op/s" % ((self.opCount / total_time))
            ret += f % (txn, txn_cnt, str(txn_time * 1000), rate)
            
            #LOG.info("totalOps="+str(self.totalOps))
            # total_time += txn_time
        ret += "\n" + ("-"*total_width)
        
        rate = 0
        if total_time > 0:
            rate = self.opCount / float(total_time)
            # TXN RATE rate = total_cnt / float(total_time)
        #total_rate = "%.02f txn/s" % rate
        total_rate = "%.02f op/s" % rate
        ret += f % ("TOTAL", str(total_cnt), str(total_time * 1000), total_rate)

        return (ret.encode('utf-8'))
## CLASS