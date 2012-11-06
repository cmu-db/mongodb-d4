
from message import *
import sys
import time
import Queue

import logging
LOG = logging.getLogger(__name__)

class Coordinator:
    def __init__(self):
        self.channels = None
        self.bestCost = sys.maxint
        self.config = None
        self.bestDesign = None
        
        self.debug = False
    ## DEF
    
    def init(self, config, channels, args):
        self.channels = channels
        self.config = config
        self.args = args
        
        start = time.time()
        
        mch = execnet.MultiChannel(self.channels)
        self.queue = mch.make_receive_queue()
    
        # Tell every client to start
        self.send2All(MSG_CMD_INIT, (config, args))
        
        ## FOR
        
        # Count down until all clients are initialized
        num_clients = len(self.channels)
        while True:
            try:
                chan, res = self.queue.get(timeout=60)
                assert getMessage(res).header == MSG_INIT_COMPLETED
                num_clients -= 1
                if num_clients == 0:
                    break
            except Exception:
                LOG.info("WAITING, clients left: %s, elapsed time: %s", num_clients, time.time() - start)
                pass
        ## WHILE
        
        LOG.info("All clients are initialized")
        LOG.info("Loading time: %s", time.time() - start)
    ## DEF
    
    def sendLoadDBCommand(self):
        LOG.info("Sending out load database commands")
        self.send2All(MSG_CMD_LOAD_DB, None)
        
        bestInitCost = sys.maxint
        bestInitDesign = None
        num_of_response = 0
        
        while True:
            try:
                chan, res = self.queue.get(timeout=60)
                msg = getMessage(res)
                if msg.header == MSG_INITIAL_DESIGN:
                    num_of_response += 1
                    LOG.info("Got one initial design")
                    if msg.data[0] < bestInitCost:
                        bestInitCost = msg.data[0]
                        bestInitDesign = msg.data[1].copy()
                    ## IF
                    if num_of_response == len(self.channels):
                        LOG.info("Got all responses and found the best initial design. Distribute it to all clients...")
                        break
                    ## IF
                else:
                    LOG.info("INVALID command %s", msg.header)
                    exit("CUPCAKE")
            except Queue.Empty:
                LOG.info("WAITING, Got %d responses", num_of_response)
        ## WHILE
        
        assert bestInitCost != sys.maxint
        assert bestInitDesign
        
        self.bestCost = bestInitCost
        self.bestDesign = bestInitDesign
        
        return bestInitCost, bestInitDesign
    ## DEF
    
    def sendExecuteCommand(self, bestInitCost, bestInitDesign):
        self.send2All(MSG_CMD_EXECUTE, (bestInitCost, bestInitDesign))
        
        running_clients = len(self.channels)
        started_searching_process = 0
        evaluated_design = 0
        finished_update = 0
        num_bestDesign = 0
        
        while True:
            try:
                chan, res = self.queue.get(timeout=60)
                msg = getMessage(res)
                
                if msg.header == MSG_EXECUTE_COMPLETED:
                    running_clients -= 1
                    LOG.info("One process has terminated, there are %d left.", running_clients)
                    if running_clients == 0:
                        break
                ## IF
                elif msg.header == MSG_EVALUATED_ONE_DESIGN:
                    evaluated_design += 1
                    if self.debug:
                        LOG.info("Best cost: %s", msg.data[0])
                        LOG.info("Evaluated cost: %s", msg.data[1])
                        
                    # Output current status every 1000 evaluation
                    if evaluated_design % 1000 == 0:
                        raise Queue.Empty
                    
                ## ELIF
                elif msg.header == MSG_FOUND_BEST_COST:
                    bestCost = msg.data[0]
                    bestDesign = msg.data[1]
                    
                    if bestCost < self.bestCost:
                        LOG.info("Got new best design. Distribute it!")
                        LOG.info("Best cost is updated from %s to %s", self.bestCost, bestCost)
                        LOG.info("New best design\n%s", bestDesign)
                        num_bestDesign += 1
                        
                        self.bestCost = bestCost
                        self.bestDesign = bestDesign.copy()
                        finished_update = 0
                        self.send2All(MSG_CMD_UPDATE_BEST_COST, (bestCost, bestDesign))
                    ## IF
                ## ELIF
                elif msg.header == MSG_START_SEARCHING:
                    LOG.info("One process started searching, we are good :)")
                    started_searching_process += 1
                    if started_searching_process == len(self.channels):
                        LOG.info("Perfect! All the processes have started searching")
                ## ELIF
                elif msg.header == MSG_FINISHED_UPDATE:
                    LOG.info("One process finished updating new best cost")
                    finished_update += 1
                    if finished_update == len(self.channels):
                        LOG.info("Perfect! All the processes have finished update")
                else:
                    LOG.info("Got invalid command: %s", msg.header)
                    exit("CUPCAKE")
                    
            except Queue.Empty:
                LOG.info("WAITING, clients left: %s", running_clients)
                LOG.info("Number of evaluated design: %d", evaluated_design)
                LOG.info("Found %s better designs so far", num_bestDesign)
                LOG.info("Best cost: %s", self.bestCost)
                LOG.info("Best Design:\n%s", self.bestDesign)
        ## WHILE
    def execute(self):
        """
            send messages to channels to tell them to start running
            update the local best cost and distribute the new values to every channel
        """ 
        start = time.time()
        # STEP 0. Tell the clients to load the database from mongodb and generate their own initial design
        bestInitCost, bestInitDesign = self.sendLoadDBCommand()
        
        # STEP 1. Tell the clients to start the search algorithm from the same initial design
        self.sendExecuteCommand(bestInitCost, bestInitDesign)
        
        end = time.time()
        LOG.info("All the clients finished executing")
        LOG.info("Best cost: %s", self.bestCost)
        LOG.info("Best design: \n%s", self.bestDesign)
        LOG.info("Time elapsed: %s", end - start)
    ## DEF
    
    def send2All(self, cmd, message):
        for channel in self.channels:
            sendMessage(cmd, message, channel)
        ## FOR
    ## DEF
    
## CLASS