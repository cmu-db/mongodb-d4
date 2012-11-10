import execnet
import argparse
import logging
import os
import sys
from ConfigParser import RawConfigParser

LOG = logging.getLogger(__name__)

basedir = os.getcwd()
sys.path.append(os.path.join(basedir, "../../../src"))

from util import constants
from util import configutil
from messageprocessor import MessageProcessor
from replaycoordinator import ReplayCoordinator
from denormalizer import Denormalizer

class Replay:
    def __init__(self, config):
        self.num_workers = config.getint(configutil.SECT_REPLAY, 'num_workers')
        self.coordinator = ReplayCoordinator()
        self.channels = None
        self.config = config
    ## DEF
    
    def run(self):
        # STEP 0: Reconstruct the database based on the design
        self.channels = self.createChannels(self.num_workers)
        # STEP 1: Tell all the workers to initialize
        self.coordinator.init(self.config, self.channels)
        # STEP 1: Reconstruct the database and send the new workload to all workers
        self.coordinator.prepare()
        # STEP 2: Tell all the workers to start their execution
        self.coordinator.execute()
    ## DEF
    
    def createChannels(self, num):
        LOG.info("Invoking benchmark framework on %d clients" % num)

        import replay
        remoteCall = replay
        channels=[]
        
        # create channels to client nodes
        for i in xrange(num):
            gw = execnet.makegateway("popen//id=sub"+str(i))
            ch = gw.remote_exec(remoteCall)
            channels.append(ch)
        ## FOR
        
        return channels
    ## DEF

## CLASS

if __name__ == '__main__':
    aparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description="%s - Replay Main Program" % constants.PROJECT_NAME)
                                      
    # Configuration File Options
    aparser.add_argument('--config', type=file,
                         help='Path to %s configuration file' % constants.PROJECT_NAME)
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file.')

    aparser.add_argument('--input-design', type=str,
                         help='Path to final design file.')
    
    args = vars(aparser.parse_args())

    if args['print_config']:
        print configutil.formatDefaultConfig()
        sys.exit(0)
    
    if not args['config']:
        LOG.error("Missing configuration file")
        print
        aparser.print_usage()
        sys.exit(1)
        
    LOG.info("Loading configuration file '%s'" % args['config'])
    config = RawConfigParser()
    configutil.setDefaultValues(config)
    config.read(os.path.realpath(args['config'].name))
    
    r = Replay(config)
    # Bombs away
    r.run()
## IF

## ==============================================
## EXECNET PROCESSOR
## ==============================================
if __name__ == '__channelexec__':
    mp = MessageProcessor(channel)
    mp.processMessage()