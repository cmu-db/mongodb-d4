class AbstractWorker:
    '''abbstract worker '''
    def __init__(self):
	''' All subclass constructor should not take any argument. You can do more initializing work in initializing method '''
	pass
    
    def initialize(config,channel,msg):
	''' initialization. You might want to send a INIT_COMPLETED message back''' 
	return None

    def startLoading(config,channel,msg):
	''' Actual loading. You might want to send a LOAD_COMPLETED message back with the loading time'''
	return None
	
    def startExecution(config,channel,msg):
	''' Actual execution. You might want to send a EXECUTE_COMPLETED message back with the loading time'''
	return None
	
    def moreProcessing(config,channel,msg):
	'''hook'''
	return None


