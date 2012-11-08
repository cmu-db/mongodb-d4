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

import threading
import logging

LOG = logging.getLogger(__name__)

class MongoStatCollector(threading.Thread):
    
    def __init__(self, sshHost, sshUser, sshOpts, updateInterval=10):
        threading.Thread.__init__(self)
        self.sshHost = sshHost
        self.sshUser = sshUser
        self.sshOpts = sshOpts
        self.updateInterval = updateInterval
        self.daemon = True
        self.process = None
        self.outputFile = "mongostat.log" # FIXME
    ## DEF
    
    def run(self):
        sshOptsStr = " ".join(map(lambda k: "-o \"%s %s\"" % (k, self.sshOpts[k]), self.sshOpts.iterkeys()))
        command = "ssh %s@%s %s \"%s\"" % (SSH_USER, host, sshOpts, "mongostat")
        
        LOG.info("Forking command: %s" % command)
        self.process = subprocess.Popen(command, \
                             stdout=subprocess.PIPE, \
                             stderr=subprocess.PIPE, \
                             shell=True)
        output, errors = self.process.communicate()
        
        LOG.info("Writing output to '%s'" % self.outputFile)
        try:
            with open(self.outputFile, "w") as fd:
                for line in output:
                    fd.write(line)
        finally:
            self.process.terminate()
    ## DEF
    
    def stop(self):
        if not self.process is None:
            self.process.terminate()
    ## DEF

## CLASS
