
from message import *
import sys
import time

import logging
LOG = logging.getLogger(__name__)

class Coordinator:
    def __init__(self):
        self.channels = None
        self.bestCost = sys.maxint
        self.config = None
        self.bestDesign = None
    ## DEF
    
    def init(self, config, channels, args):
        self.channels = channels
        self.config = config
        self.args = args
        
        mch = execnet.MultiChannel(self.channels)
        self.queue = mch.make_receive_queue()
    
        # Tell every client to start
        self.send2All(MSG_CMD_INIT, (config, args))
        
        ## FOR
        
        # Count down until all clients are initialized
        num_clients = len(self.channels)
        while True:
            try:
                chan, res = self.queue.get(timeout=1)
                assert getMessage(res).header == MSG_INIT_COMPLETED
                num_clients -= 1
                if num_clients == 0:
                    break
            except Exception:
                LOG.info("WAITING, clients left: %s", num_clients)
                pass
        ## WHILE
        
        LOG.info("All clients are initialized")
    ## DEF
    
    def execute(self):
        """
            send messages to channels to tell them to start running
            update the local best cost and distribute the new values to every channel
        """ 
        # Tell all clients to run search algorithm
        start = time.time()
        self.send2All(MSG_CMD_EXECUTE, None)
        
        running_clients = len(self.channels)
        started_process = 0
        started_searching_process = 0
        
        while True:
            distrubute_value = True
            try:
                chan, res = self.queue.get(timeout=60)
                data = getMessage(res)
                
                if data.header == MSG_EXECUTE_COMPLETED:
                    running_clients -= 1
                    LOG.info("One process has terminated, there are %d left.", )
                    if running_clients == 0:
                        break
                elif data.header == MSG_NEW_BEST_COST:
                    bestCost = data[0]
                    bestDesign = data[1]
                    
                    if self.bestCost > bestCost:
                        if self.bestCost == sys.maxint:
                            distrubute_value = False
                        self.bestCost = bestCost
                        self.bestDesign = bestDesign.copy()
                    ## IF
                    
                    if distrubute_value:
                        self.send2All(MSG_CMD_UPDATE_BEST_COST, (bestCost, bestDesign))
                    ## IF
                elif data.header == MSG_START_EXECUTING:
                    LOG.info("One process is started, we are good :)")
                    started_process += 1
                    if started_process == len(self.channels):
                        LOG.info("Perfect! All the processes are running!")
                elif data.header == MSG_START_SEARCHING:
                    LOG.info("One process started searching, we are good :)")
                    started_searching_process += 1
                    if started_searching_process == len(self.channels):
                        LOG.info("Perfect! All the processes have started searching")
                else:
                    LOG.info("Got invalid command: %s", data.header)
                    exit("CUPCAKE")
                    
            except Exception:
                LOG.info("WAITING, clients left: %s", running_clients)
                LOG.info("Best cost: %s", self.bestCost)
                LOG.info("Best Design:\n%s", self.bestDesign)
        ## WHILE
        end = time.time()
        LOG.info("All the clients finished executing")
        LOG.info("Best cost: %s", self.bestCost)
        LOG.info("Best design: %s", self.bestDesign)
        LOG.info("Time elapsed: %s", end - start)
    ## DEF
    
    def send2All(self, cmd, message):
        for channel in self.channels:
            sendMessage(cmd, message, channel)
        ## FOR
    ## DEF
    
## CLASS