#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Yang Lu + Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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
from __future__ import with_statement

import sys
import os
import string
import re
import glob
import subprocess
import execnet
import logging
import time
import threading
from datetime import datetime
from ConfigParser import SafeConfigParser
from pymongo import Connection
from pprint import pprint, pformat

from api.messageprocessor import *
from api.directchannel import *
from api.mongostat import *

# Third-Party Dependencies
if __name__ == '__channelexec__':
    # Remote execnet invocations won't have a __file__
    BASEDIR = os.getcwd()
else:
    BASEDIR = os.path.realpath(os.path.dirname(__file__))
for d in ["src", "libs"]:
    dir = os.path.realpath(os.path.join(BASEDIR, "..", d))
    if not dir in sys.path:
        sys.path.append(dir)
## FOR
import argparse
import mongokit

# We need to load up the stuff that our execnet threads will need
from util.histogram import Histogram

# LOG = logging.getLogger(__name__)
LOG = logging.getLogger()

SSH_USER = "ubuntu"
SSH_OPTIONS = {
    "UserKnownHostsFile": "/dev/null",
    "StrictHostKeyChecking": "no",
    "RequestTTY": "force",
}

## ==============================================
## Benchmark Invocation
## ==============================================
class Benchmark:
    DEFAULT_CONFIG = [
        ("dbname", "The name of the MongoDB database to use for this benchmark invocation.", None), 
        ("host", "The host name of the MongoDB instance to use in this benchmark", "localhost"),
        ("port", "The port number of the MongoDB instance to use in this benchmark", 27017),
        ("scalefactor", "Benchmark database scale factor", 1.0),
        ("duration", "Benchmark execution time in seconds", 60),
        ("warmup", "Benchmark warm-up period", 60),
        ("clients", "Comma-separated list of machines to use for benchmark clients", "localhost"),
        ("clientprocs", "Number of worker processes to spawn on each client host.", 1),
        ("design", "Path to database design file (must be supported by benchmark).", ""),
        ("logfile", "Path to debug log file for remote execnet processes", None),
        ("mongostat_sleep", "The number of seconds to sleep before collecting new info using mongostat", 10),
        ("restart","if true it will restart the mongod server(s)",True), 
    ]
    
    '''main class'''
    def __init__(self, benchmark, args):
        self._benchmark = benchmark
        
        # Fix database name + logfile
        for i in xrange(len(self.DEFAULT_CONFIG)):
            c = self.DEFAULT_CONFIG[i]
            if c[0] == 'dbname':
                self.DEFAULT_CONFIG[i] = (c[0], c[1], self._benchmark.lower())
            elif c[0] == 'logfile':
                logfile = os.path.join("/tmp", "%s.log" % self._benchmark.lower())
                self.DEFAULT_CONFIG[i] = (c[0], c[1], logfile)
        ## FOR
        
        self.args = args
        self.coordinator = self.createCoordinator()
        self.config = None
        self.channels = None
    ## DEF
        
    
    def makeDefaultConfig(self):
        """Return a string containing the default configuration file for the target benchmark"""
        ret =  "# %s Benchmark Configuration File\n" % (self._benchmark.upper())
        ret += "# Created %s\n" % (datetime.now())
        
        # Base Configuration
        ret += formatConfig("default", self.DEFAULT_CONFIG)
        
        # Benchmark Configuration
        ret += formatConfig(self._benchmark, self.coordinator.benchmarkConfigImpl())

        return (ret)
    ## DEF
    
    def loadConfig(self):
        '''Load configuration file'''
        assert 'config' in self.args
        assert self.args['config'] != None
        #print("~~~")
	print(self.args['config'].name)
        cparser = SafeConfigParser()
        cparser.read(os.path.realpath(self.args['config'].name))
        config = dict()
        for s in cparser.sections():
            config[s] = dict(cparser.items(s))
        ## FOR
        
        config['default']['name'] = self._benchmark
        
        # Extra stuff from the arguments that we want to stash
        # in the 'default' section of the config
        for key,val in args.items():
            if key != 'config' and not val is None: # not key in config['default']:
                config['default'][key] = val
                
        # Default config
        for key, desc, default in self.DEFAULT_CONFIG:
            if key != 'config' and not key in config['default']:
                config['default'][key] = default
        
        # Figure out where the hell we actually are
        realpath = os.path.realpath(__file__)
        basedir = os.path.dirname(realpath)
        if not os.path.exists(realpath):
            cwd = os.getcwd()
            basename = os.path.basename(realpath)
            if os.path.exists(os.path.join(cwd, basename)):
                basedir = cwd
        #config['path'] = os.path.join(basedir, "api")
        config['default']['path'] = basedir
        
        # Fix common problems
        for s in config.keys():
            for key in ["port", "duration", "clientprocs"]:
                if key in config[s]: config[s][key] = int(config[s][key])
            for key in ["scalefactor"]:
                if key in config[s]: config[s][key] = float(config[s][key])
            for key in ["path", "logfile"]:
                if key in config[s]: config[s][key] = os.path.realpath(config[s][key])
        ## FOR
        
        # Read in the serialized design file and ship that over the wire
        if "design" in config["default"] and config["default"]["design"]:
            LOG.debug("Reading in design file '%s'" % config["default"]["design"])
            with open(config["default"]["design"], "r") as fd:
                config["default"]["design"] = fd.read()
        else:
            config["default"]["design"] = None
        ## IF
        
        self.config = config
        LOG.debug("Configuration File:\n%s" % pformat(self.config))
    ## DEF
            
    def runBenchmark(self):
        '''Execute the target benchmark!'''
        self.channels = self.createChannels()

        # Step 0: Flush the cache on the MongoDB host
        if self.args['flush']:
            # Check whether we have sharding nodes. If so, then we'll
            # Connect to the mongo server and get the list of shards
            hostname = self.config["default"]["host"]
            port = self.config["default"]["port"]
            try:
                conn = Connection(host=hostname, port=port)
            except:
                LOG.error("Failed to connect to MongoDB at %s:%s" % (hostname, port))
                raise
            
            result = conn["admin"].command("listShards")
            if "shards" in result:
                shards = set()
                for entry in result["shards"]:
                    shardHost,shardPort = entry["host"].split(":")
                    shards.add(shardHost)
                # Restart these mofos
                LOG.warn("Restarting MongoDB Shards: %s", list(shards))
                map(flushBuffer, shards)
            # Otherwise, just restart the front end node
            else:
                flushBuffer(self.config["default"]["host"])
        
        # Step 1: Initialize all of the Workers on the client nodes
        self.coordinator.init(self.config, self.channels) 
        
        # Step 2: Load the benchmark database
        if not self.args['no_load']:
            self.coordinator.load(self.config, self.channels)            
            
        # Step 3: Execute the benchmark workload
        if not self.args['no_execute']:
            mongostat = None
            try:
                # Start mongostat
                if self.args["mongostat"]:
                    dbHost = self.config["default"]["host"]
                    mongostat = MongoStatCollector(dbHost, SSH_USER, SSH_OPTIONS)
                    mongostat.start()
                self.coordinator.execute(self.config, self.channels)
                self.coordinator.showResult(self.config, self.channels)
            finally:
                if not mongostat is None:
                    mongostat.stop()
        ## IF
            
        # Step 4: Clean things up (?)
        # self.coordinator.moreProcessing(self.config, self.channels)
    ## DEF
        
    def createChannels(self):
        '''Create a list of channels used for communication between coordinator and worker'''
        assert 'clients' in self.config['default']
        clients = re.split(r"[\s,]+", str(self.config['default']['clients']))
        assert len(clients) > 0
        LOG.info("Invoking benchmark framework on %d clients" % len(clients))

        import benchmark
        remoteCall = benchmark
        channels=[]
        
        # Create fake channel that invokes the worker directly in
        # the same process
        if self.config['default']['direct']:
            self.config['default']['clientprocs'] = 1
            ch = DirectChannel()
            channels.append(ch)
            
        # Otherwise create SSH channels to client nodes
        else:
            # Print a header message in the logfile to indicate that we're starting 
            # a new benchmark run
            LOG.info("Executor Log File: %s" %  self.config['default']['logfile'])
            with open(self.config['default']['logfile'], "a") as fd:
                header = "%s BENCHMARK - %s\n\n" % (self._benchmark.upper(), datetime.now())
                header += "%s" % (pformat(self.config))
                
                fd.write('*'*100 + '\n')
                for line in header.split('\n'):
                    fd.write('* ' + line + '\n')
                fd.write('*'*100 + '\n')
            ## WITH
            
            totalClients = len(clients) * self.config['default']['clientprocs']
            start = time.time()
            for node in clients:
                cmd = 'ssh='+ node
                cmd += r"//chdir=" + self.config['default']['path']
                LOG.debug(cmd)
                LOG.debug("# of Client Processes: %s" % self.config['default']['clientprocs'])
                for i in range(int(self.config['default']['clientprocs'])):
                    LOG.debug("Invoking %s on %s" % (remoteCall, node))
                    gw = execnet.makegateway(cmd)
                    ch = gw.remote_exec(remoteCall)
                    channels.append(ch)
                    now = time.time()
                    if (now - start) > 10 and int((len(channels) / float(totalClients))*100) % 25 == 0:
                        LOG.debug("Started Client Threads %d / %d" % (len(channels), totalClients))
                ## FOR (processes)
            ## FOR (hosts)
        # IF
        LOG.info("Created %d client processes on %d hosts" % (len(channels), len(clients)))
        LOG.debug(channels)
        return channels
    ## DEF
        
    def createCoordinator(self):
        '''Coordinator factory method.'''

        # First make sure that the benchmark is on our sys.path
        setupBenchmarkPath(self._benchmark)
        
        # Then use some black magic to instantiate an instance of the benchmark's coordinator
        fullName = self._benchmark.title() + "Coordinator"
        moduleName = "benchmarks.%s.%s" % (self._benchmark.lower(), fullName.lower())
        moduleHandle = __import__(moduleName, globals(), locals(), [fullName])
        klass = getattr(moduleHandle, fullName)
        return klass()
    ## DEF
    
    def collectMongoStat(self):
        """Spawn a thread that logs into the server and retrives mongostat output"""
        sshOpts = " ".join(map(lambda k: "-o \"%s %s\"" % (k, SSH_OPTIONS[k]), SSH_OPTIONS.iterkeys()))
        cmd = "ssh %s@%s %s \"%s\"" % (SSH_USER, host, sshOpts, "mongostat")
        
## CLASS
        
## ==============================================
## getBenchmarks
## ==============================================
def getBenchmarks():
    """Return a list of the valid benchmark handles that can be used in the framework"""
    benchmarks = [ ]
    for f in glob.glob("./benchmarks/*"):
        if os.path.isdir(f): benchmarks.append(os.path.basename(f).strip())
    return (benchmarks)
## DEF

## ==============================================
## setupBenchmarkPath
## ==============================================
def setupBenchmarkPath(benchmark):
    benchmarkDir = os.path.realpath(os.path.join(BASEDIR, "benchmarks", benchmark))
    if not benchmarkDir in sys.path:
        sys.path.insert(0, benchmarkDir)
## DEF

## ==============================================
## flushBuffer
## ==============================================
def flushBuffer(host):
    if self.config['restart']:
        remoteCmds = [
            #"sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'",
            #"sudo service mongodb restart",
            #"ssh -t ubuntu@"+host+" -o \"UserKnownHostsFile /dev/null\" " + "-o \"StrictHostKeyChecking no\""+"  \"sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'\"",
            #"ssh -t ubuntu@"+host+" -o \"UserKnownHostsFile /dev/null\" " + "-o \"StrictHostKeyChecking no\""+"  \"sudo service mongod restart\"",
            "sudo service mongod stop",
            "sudo service mongod start",
            "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'",
        ]
    else:
        remoteCmds = [
            "sudo service mongod stop",
            "sudo service mongod start",
        ]
    LOG.info("Flushing OS cache and restart MongoDB on host '%s'" % host)
    
    sshOpts = " ".join(map(lambda k: "-o \"%s %s\"" % (k, SSH_OPTIONS[k]), SSH_OPTIONS.iterkeys()))
    baseCmd = "ssh %s@%s %s" % (SSH_USER, host, sshOpts)
    for cmd in remoteCmds:
        subprocess.check_call("%s \"%s\"" % (baseCmd, cmd), shell=True)
    time.sleep(60)
    ## FOR
## DEF

## ==============================================
## formatConfig
## ==============================================
def formatConfig(name, config):
    """Return a formatted version of the config list that can be used with the --config command line argument. See AbstractCoordinator.benchmarkConfigImpl() for what the tuples in this list look like."""

    # Header
    ret = "\n# " + ("="*75) + "\n"
    ret += "[%s]" % name
    
    # Benchmark Configuration
    for key, desc, default in config:
        if default == None: default = ""
        ret += "\n\n# %s\n%-20s = %s" % (desc, key, default) 
    ret += "\n"
    return (ret)
## DEF

## ==============================================
## MAIN
## ==============================================
if __name__=='__main__':
    from util import termcolor
    logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
    
    allBenchmarks = getBenchmarks()
    
    # Simplified args
    aparser = argparse.ArgumentParser(description='MongoDB Benchmark Framework')
    aparser.add_argument('benchmark', choices=allBenchmarks,
                         help='The name of the benchmark to execute.')
                         
    # Configuration Options
    agroup = aparser.add_argument_group(termcolor.bold('Configuration Options'))
    agroup.add_argument('--config', type=file,
                         help='Path to benchmark configuration file.')
    agroup.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the benchmark and exit.')
                         
    # Config Override Parameters
    override = {
        "scalefactor": ("SF", float),
        "duration":    ("D", int),
        "clients":     ("C", str),
        "clientprocs": ("N", int),
    }
    for key,desc,default in Benchmark.DEFAULT_CONFIG:
        if not key in override: continue
        
        argMeta, argType = override[key]
        agroup.add_argument('--' + key, \
                             type=argType, \
                             metavar=argMeta, \
                             help=desc)
    ## FOR
                
    # Database Parameters
    agroup = aparser.add_argument_group(termcolor.bold('Database Options'))
    agroup.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    agroup.add_argument('--flush', action='store_true',
                        help="Flush the OS cache on the MongoDB host before executing the benchmark. This requires passwordless sudo access.")
    agroup.add_argument('--mongostat', action='store_true',
                        help="Execute mongostat on database host and retrieve the results.")
    agroup.add_argument('--no-load', action='store_true',
                         help='Disable loading the benchmark data')
    agroup.add_argument('--no-execute', action='store_true',
                         help='Disable executing the benchmark workload')

    # Debugging Options
    agroup = aparser.add_argument_group(termcolor.bold('Debugging Options'))
    agroup.add_argument('--direct', action='store_true',
                         help='Execute the workers directly in this process')                            
    agroup.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    agroup.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())
    
    if args['debug']: LOG.setLevel(logging.DEBUG)
    
    benchmark = args['benchmark'].lower()
    if not benchmark in allBenchmarks:
        raise Exception("Invalid benchmark handle '%s'" % benchmark)
    
    LOG.debug("Command Options:\n%s" % args)
    ben = Benchmark(benchmark, args)
    
    if args['print_config']:
        print ben.makeDefaultConfig()
        sys.exit(0)
    
    # Run it!
    ben.loadConfig()
    ben.runBenchmark()
## MAIN

## ==============================================
## EXECNET PROCESSOR
## ==============================================
if __name__ == '__channelexec__':
    #import pycallgraph
    #import os
    #pycallgraph.start_trace()
    #pid=os.getpid()
    #try:
        mp = MessageProcessor(channel)
        mp.processMessage()
    #finally:
    #    pycallgraph.make_dot_graph("d4-"+str(pid)+".png")
    #    pass
## EXEC
 
