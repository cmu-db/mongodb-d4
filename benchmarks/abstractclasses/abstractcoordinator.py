class AbstractCoordinator:
    '''Abstract coordinator.'''
    def __init__(self):
	'''All subclass constructor should not taken any arguments. You can do more initializing work in initialize() method'''
	pass
    
    def initialize(self,config,channels):
	'''initialize method. It is recommanded that you send the a CMD_INIT message with the config object to the client side in the method'''
	
	#see message.py for more information
	return None	
	
    def distributeLoading(self,config,channels):
	''' distribute loading to a list of channels by sending command message to each of them.\
	You can collect the load time from each channel and returns the total loading time'''
	
	##How to use channel object, see http://codespeak.net/execnet/examples.html
	##See also ExampleCoordinator.py
	
        return None
        
    def distributeExecution(self,config,channels):
	'''distribute execution to a list of channels by send command message to each of them.\
	You can collect the execution result from each channel'''
	
	##How to use channel object, see http://codespeak.net/execnet/examples.html
	##See also ExampleCoordinator.py
	
	return None
	
    def showResult(self,config,channels):
	'''optional result display method'''
        return None
       
    def moreProcessing(self,config,channels):
        '''hook'''
        return None
 