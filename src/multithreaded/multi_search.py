
import os
import sys
import logging
import execnet

# Third-Party Dependencies
# Remote execnet invocations won't have a __file__
basedir = os.getcwd()
sys.path.append(os.path.join(basedir, ".."))
sys.path.append(os.path.join(basedir, "../search"))

from search import Designer
from multi_search_coordinator import Coordinator
from util import configutil

LOG = logging.getLogger(__name__)

class MultiClientDesigner:
    """
        This is the multithreaded version of LNS search
    """
    def __init__(self, config, args):
        self.config = config
        self.args = args # ONLY USED FOR Designer.setOptionsFromArguments: Comment: this is a weired method
        self.coordinator = Coordinator()
        self.channels = None
    ## DEF
            
    def runSearch(self):
        '''Execute the target benchmark!'''
        self.channels = self.createChannels()
        
        # Step 1: Initialize all of the Workers on the client nodes
        self.coordinator.init(self.config, self.channels, self.args)
            
        # Step 2: Execute search 
        self.coordinator.execute()
    ## DEF
    
    def createChannels(self):
        '''Create a list of channels used for communication between coordinator and worker'''
        num_clients = self.config.getint(configutil.SECT_MULTI_SEARCH, 'num_clients')
        LOG.info("Invoking benchmark framework on %d clients" % num_clients)

        import d4
        remoteCall = d4
        channels=[]
        
        # create channels to client nodes
        for i in xrange(num_clients):
            gw = execnet.makegateway("popen//id=sub"+str(i))
            ch = gw.remote_exec(remoteCall)
            channels.append(ch)
        ## FOR (hosts)
        
        LOG.debug(channels)
        return channels
    ## DEF
    
## CLASS