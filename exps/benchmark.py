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

import sys
import os
import string
import re
import glob
import execnet
import logging
from ConfigParser import SafeConfigParser
from pprint import pprint, pformat

from api.messageprocessor import *
from api.directchannel import *

# Third-Party Dependencies
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../libs"))
import argparse
import mongokit

# MongoDB-Designer
sys.path.append(os.path.join(basedir, "../src"))

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(filename)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)


## ==============================================
## Benchmark Invocation
## ==============================================
class Benchmark:
    DEFAULT_CONFIG = {
        "host":     ("The host name of the MongoDB instance to use in this benchmark", "localhost"),
        "port":     ("The port number of the MongoDB instance to use in this benchmark", 27017),
    }
    
    '''main class'''
    def __init__(self, benchmark, args):
        self._benchmark = benchmark
        self._args = args
        self._coordinator = self.createCoordinator()
        self._config = None
        self._channels = None
    ## DEF
        
    
    def makeDefaultConfig(self):
        """Return a string containing the default configuration file for the target benchmark"""

        from datetime import datetime
        ret =  "# %s Benchmark Configuration File\n" % (self._benchmark.upper())
        ret += "# Created %s\n" % (datetime.now())
        
        # Base Configuration
        ret += formatConfig("default", self.DEFAULT_CONFIG)
        
        # Benchmark Configuration
        ret += formatConfig(self._benchmark, self._coordinator.benchmarkConfigImpl())

        return (ret)
    ## DEF
        
    def runBenchmark(self):
        '''Execute the target benchmark!'''
        self._channels = self.createChannels()
        
        # Step 1: Initialize all of the Workers on the client nodes
        self._coordinator.init(self._config, self._channels) 
        
        # Step 2: Load the benchmark database
        if not self._args['no_load']:
            self._coordinator.load(self._config, self._channels)            
            
        # Step 3: Execute the benchmark workload
        if not self._args['no_execute']:
            self._coordinator.execute(self._config, self._channels)
            self._coordinator.showResult(self._config, self._channels)
            
        # Step 4: Clean things up (?)
        # self._coordinator.moreProcessing(self._config, self._channels)
    ## DEF
    
    def loadConfig(self):
        '''Load configuration file'''
        assert 'config' in self._args
        assert self._args['config'] != None
        
        cparser = SafeConfigParser()
        cparser.read(os.path.realpath(self._args['config'].name))
        config = dict()
        for s in cparser.sections():
            config[s] = dict(cparser.items(s))
        ## FOR
        
        # Extra stuff from the arguments that we want to stash
        # in the 'default' section of the config
        for key,val in args.items():
            if key != 'config' and not key in config['default']:
                config['default'][key] = val
        config['default']['name'] = self._benchmark
        
        # Figure out where the hell we actually are
        realpath = os.path.realpath(__file__)
        basedir = os.path.dirname(realpath)
        if not os.path.exists(realpath):
            cwd = os.getcwd()
            basename = os.path.basename(realpath)
            if os.path.exists(os.path.join(cwd, basename)):
                basedir = cwd
        #config['path'] = os.path.join(basedir, "api")
        config['path'] = os.path.realpath(basedir)
        
        # Fix common problems
        for s in config.keys():
            for key in ["port", "duration", "experiment"]:
                if key in config[s]: config[s][key] = int(config[s][key])
        ## FOR
        
        # Read in the serialized design file and ship that over the wire
        if "design" in config["default"] and config["default"]["design"]:
            LOG.debug("Reading in design file '%s'" % config["default"]["design"])
            with open(config["default"]["design"], "r") as fd:
                config["default"]["design"] = fd.read()
        else:
            config["default"]["design"] = None
        ## IF
        
        logging.info("Configuration File:\n%s" % pformat(config))
        return config
    ## DEF
        
    def createChannels(self):
        '''Create a list of channels used for communication between coordinator and worker'''
        assert 'clients' in self._config['default']
        clients = re.split(r"\s+", str(self._config['default']['clients']))
        assert len(clients) > 0
        logging.info("Invoking benchmark framework on %d clients" % len(clients))

        import benchmark
        remoteCall = benchmark
        channels=[]
        
        # Create fake channel that invokes the worker directly in
        # the same process
        if self._config['default']['direct']:
            ch = DirectChannel()
            channels.append(ch)
            
        # Otherwise create SSH channels to client nodes
        else:
            for node in clients:
                cmd = 'ssh='+ node
                cmd += r"//chdir="
                cmd += self._config['default']['path']
                logging.debug(cmd)
                logging.debug("# of Client Processes: %s" % self._config['default']['clientprocs'])
                for i in range(int(self._config['default']['clientprocs'])):
                    logging.debug("Invoking %s on %s" % (remoteCall, node))
                    gw = execnet.makegateway(cmd)
                    ch = gw.remote_exec(remoteCall)
                    channels.append(ch)
        # IF
        logging.debug(channels)
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

## CLASS
        
## ==============================================
## getBenchmarks
## ==============================================
def getBenchmarks():
    benchmarks = [ ]
    for f in glob.glob("./benchmarks/*"):
        if os.path.isdir(f): benchmarks.append(os.path.basename(f).strip())
    return (benchmarks)
## DEF

## ==============================================
## setupBenchmarkPath
## ==============================================
def setupBenchmarkPath(benchmark):
    realpath = os.path.realpath(__file__)
    basedir = os.path.dirname(realpath)
    if not os.path.exists(realpath):
        cwd = os.getcwd()
        basename = os.path.basename(realpath)
        if os.path.exists(os.path.join(cwd, basename)):
            basedir = cwd
    benchmarkDir = os.path.join(basedir, "benchmarks", benchmark)
    sys.path.append(os.path.realpath(benchmarkDir))
## DEF

## ==============================================
## formatConfig
## ==============================================
def formatConfig(name, config):
    """Return a formatted version of the config dict that can be used with the --config command line argument"""

    ret = "\n# " + ("="*75) + "\n"
    
    # Default Configuration
    ret += "[%s]" % name
    
    # Benchmark Configuration
    for key in config.keys():
        desc, default = config[key]
        if default == None: default = ""
        ret += "\n\n# %s\n%-20s = %s" % (desc, key, default) 
    ret += "\n"
    return (ret)
## DEF

## ==============================================
## MAIN
## ==============================================
if __name__=='__main__':
    allBenchmarks = getBenchmarks()
    
    # Simplified args
    aparser = argparse.ArgumentParser(description='MongoDB Benchmark Framework')
    aparser.add_argument('benchmark', choices=allBenchmarks,
                         help='The name of the benchmark to execute')
    aparser.add_argument('--config', type=file,
                         help='Path to benchmark configuration file')
    aparser.add_argument('--design', type=str,
                         help='Path to benchmark design file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default = 1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--duration', default = 60, type=int, metavar='D',
                         help='Benchmark execution time in seconds')
    aparser.add_argument('--direct', action='store_true',
                         help='Execute the workers directly in this process')                            
    aparser.add_argument('--clientprocs', default = 1, type=int, metavar='N',
                         help='Number of processes on each client node.')
                         
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the benchmark data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the benchmark workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the benchmark and exit')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')                       
    args = vars(aparser.parse_args())
    
    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    
    benchmark = args['benchmark'].lower()
    if not benchmark in allBenchmarks:
        raise Exception("Invalid benchmark handle '%s'" % benchmark)
    
    logging.debug("Command Options:\n%s" % args)
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
    mp = MessageProcessor(channel)
    mp.processMessage()
## EXEC
 
