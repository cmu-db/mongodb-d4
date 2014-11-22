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
import math
import json
from operator import itemgetter

from util.histogram import Histogram

LOG = logging.getLogger(__name__)

class Results:
    
    def __init__(self, config=None):
        self.start = None
        self.stop = None
        self.txn_id = 0
        self.opCount = 0
        self.completed = [ ] # (txnName, timestamp)
        self.txn_counters = Histogram()
        self.txn_times = { }
        self.running = { }
        self.config = config
        
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
        
    def stopTransaction(self, id, opCount, latencies=[]):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        
        timestamp = time.time()
        
        txn_name, txn_start = self.running[id]
        del self.running[id]
        self.completed.append((txn_name, timestamp, latencies))
        
        duration = timestamp - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration
        
        # OpCount
        if opCount is not None:
            self.opCount += opCount
        else: 
            LOG.debug("ithappens")
            
        
        # Txn Counter Histogram
        self.txn_counters.put(txn_name)
        assert self.txn_counters[txn_name] > 0
        
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Completed %s in %f sec" % (txn_name, duration))
    ## DEF

    @staticmethod
    def show_table(title, headers, table, line_width):
        cols_width = [len(header) for header in headers]
        for row in table:
            row_width = 0
            for i in range(len(headers)):
                if len(row[i]) > cols_width[i]:
                    cols_width[i] = len(row[i])
                row_width += cols_width[i]
            row_width += 4 * (len(headers) - 1)
            if row_width > line_width:
                line_width = row_width
        output = ("%s\n" % ("=" * line_width))
        output += ("%s\n" % title)
        output += ("%s\n" % ("-" * line_width))
        for i in range(len(headers)):
            header = headers[i]
            output += ("%s%s" % (header, " " * (cols_width[i] - len(header))))
            if i != len(headers) - 1:
                output += " " * 4
        output += "\n"
        for row in table:
            for i in range(len(headers)):
                cell = row[i]
                output += ("%s%s" % (cell, " " * (cols_width[i] - len(cell))))
                if i != len(headers) - 1:
                    output += " " * 4
            output += "\n"
        output += ("%s\n" % ("-" * line_width))
        return output, line_width

    def show_latencies(self, line_width):
        latencies = []
        output = ""
        for txn_stats in self.completed:
            latencies.extend(txn_stats[2])
        if len(latencies) > 0:
            latencies = sorted(latencies, key=itemgetter(0))
            percents = [0.1, 0.2, 0.5, 0.8, 0.9, 0.999]
            latency_table = []
            slowest_ops = []
            for percent in percents:
                index = int(math.floor(percent * len(latencies)))
                percent_str = "%0.1f%%" % (percent * 100)
                millis_sec_str = "%0.4f" % (latencies[index][0])
                latency_table.append((percent_str, millis_sec_str))
            latency_headers = ["Queries(%)", "Latency(ms)"]
            output, line_width = \
                Results.show_table("Latency Report", latency_headers, latency_table, line_width)
            if self.config is not None and self.config["default"]["slow_ops_num"] > 0:
                num_ops = self.config["default"]["slow_ops_num"]
                slowest_ops_headers = ["#", "Latency(ms)", "Session Id", "Operation Id", "Type", "Collection", "Predicates"]
                for i in range(num_ops):
                    if i < len(latencies):
                        slowest_ops.append([
                            "%d" % i,
                            "%0.4f" % (latencies[len(latencies) - i - 1][0]),
                            str(latencies[len(latencies) - i - 1][1]),
                            str(latencies[len(latencies) - i - 1][2]),
                            latencies[len(latencies) - i - 1][3],
                            latencies[len(latencies) - i - 1][4],
                            json.dumps(latencies[len(latencies) - i - 1][5])
                        ])
                slowest_ops_output, line_width = \
                    Results.show_table("Top %d Slowest Operations" % num_ops, slowest_ops_headers, slowest_ops, line_width)
                output += ("\n%s" % slowest_ops_output)
        return output

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
            rate = u"%.02f op/s" % ((self.opCount / total_time))
            ret += f % (txn, txn_cnt, str(txn_time * 1000), rate)
            
            #LOG.info("totalOps="+str(self.totalOps))
            # total_time += txn_time
        ret += "\n" + ("-"*total_width)
        
        rate = 0
        if total_time > 0:
            rate = total_cnt / float(total_time)
            # TXN RATE rate = total_cnt / float(total_time)
        #total_rate = "%.02f txn/s" % rate
        total_rate = "%.02f op/s" % rate
        #total_rate = str(rate)
        ret += f % ("TOTAL", str(total_cnt), str(total_time*1000), total_rate)

        return ("%s\n%s" % (ret, self.show_latencies(total_width))).encode('utf-8')
## CLASS
