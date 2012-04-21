import execnet
from message import *

class MsgProcessor:
    ''' Message Processor'''
    def __init__(self,channel):
	self._channel=channel
	self._worker=None
	self._config=None
	
    def createWorker(self):
	'''Worker factory method'''
	benchmark=self._config['benchmark']
	fullName= benchmark.title()+"Worker"
	mod=__import__('%s.%s' %(benchmark.lower(),fullName.lower()), globals(), locals(), [fullName])
	klass=getattr(mod,fullName)
	return klass()
	
    def processMessage(self):
	'''Main loop'''
	for item in self._channel:
	    msg=getMessage(item)
	    if msg.header == CONFIG :
		self._config=msg.data
		self._worker=self.createWorker()
	    elif msg.header == CMD_INIT :		
		self._worker.initialize(self._config,self._channel,msg)
	    elif msg.header == CMD_LOAD :
		self._worker.startLoading(self._config,self._channel,msg)
	    elif msg.header == CMD_EXECUTE :
		self._worker.startExecution(self._config,self._channel,msg)
	    elif msg.header == CMD_STOP :
		pass
	    elif msg.header == EMPTY :
		pass
	    else:
		return

if __name__=='__channelexec__':
    
    mp = MsgProcessor(channel)
   
    mp.processMessage()