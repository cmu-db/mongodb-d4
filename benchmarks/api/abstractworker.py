# -*- coding: utf-8 -*-
import time
import execnet
from .message import *

class AbstractWorker:
    '''Abstract Benchmark Worker'''
    def __init__(self):
        ''' All subclass constructor should not take any argument. You can do more initializing work in initializing method '''
        self._sf = None
        self._driver = None
        pass
    ## DEF
    
    def initialize(config, channel, msg):
        '''Work Initialization. You always must send a INIT_COMPLETED message back'''
        self._sf = msg.data
        
        ## Create a handle to the target client driver
        driverClass = self.createDriverClass(config['system'])
        assert driverClass != None, "Failed to find '%s' class" % config['system']
        driver = driverClass(config['ddl'])
        assert driver != None, "Failed to create '%s' driver" % config['system']
        driver.loadConfig(config)
        self._driver = driver
        sendMessage(INIT_COMPLETED, None, channel)
    ## DEF
        
    def createDriverClass(self, name):
        full_name = "%sDriver" % name.title()
        mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
        klass = getattr(mod, full_name)
        return klass
    ## DEF

    def startLoading(config,channel,msg):
        ''' Actual loading. You might want to send a LOAD_COMPLETED message back with the loading time'''
        return None
        
    def startExecution(config,channel,msg):
        ''' Actual execution. You might want to send a EXECUTE_COMPLETED message back with the loading time'''
        return None
        
    def moreProcessing(config,channel,msg):
        '''hook'''
        return None


