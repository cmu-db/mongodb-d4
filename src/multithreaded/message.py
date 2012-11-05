# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Yang Lu - http://www.cs.brown.edu/~yanglu/
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
import time
import execnet
import random
import logging
import threading
try:
   import cPickle as pickle
except:
   import pickle

LOG = logging.getLogger(__name__)
   
# All of the strings in this list will become
# status codes that are prefixed with "MSG_"
# The values of these codes will all be unique
MSG_STATUS_CODES = [
    "NOOP",
    "CMD_INIT",
    "CMD_EXECUTE",
    "INIT_COMPLETED",
    "EXECUTE_COMPLETED",
    "NEW_BEST_COST",
    "CMD_UPDATE_BEST_COST",
    "START_EXECUTING",
    "START_SEARCHING"
]

MSG_NAME_MAPPING = { }
for code in xrange(0, len(MSG_STATUS_CODES)):
    name = "MSG_%s" % MSG_STATUS_CODES[code]
    globals()[name] = code
    MSG_NAME_MAPPING[code] = name
## FOR

    
def getChannelsByHost(channels):
    ret = { }
    for ch in channels:
        if ch.gateway is None:
            remoteaddress = 'direct'
        else:
            remoteaddress = ch.gateway.remoteaddress
        if not remoteaddress in ret:
            ret[remoteaddress] = [ ]
        ret[remoteaddress].append(ch)
    ## FOR
    return (ret)
## DEF

def sendMessagesLimited(queue, limit):
    start = time.time()
    responses = [ ]
    threads = [ ]

    # Send directly
    hostChannels = getChannelsByHost([x[-1] for x in queue])
    if 'direct' in hostChannels and len(hostChannels) == 1:
        sendMessagesLimitedThread(queue, limit, responses)
    # Split the queue by channel host
    else:
        for key in hostChannels.keys():
            hostQueue = [ ]
            for i in xrange(0, len(queue)):
                if queue[i][-1] in hostChannels[key]:
                    hostQueue.append(queue[i])
            ## FOR
            t = threading.Thread(target=sendMessagesLimitedThread, args=(hostQueue, limit, responses))
            t.start()
            threads.append(t)
        ## FOR
        for t in threads: t.join()

    duration = time.time() - start
    LOG.debug("Sent and recieved %d messages in %.2f seconds" % (len(responses), duration))
    return (responses)
    
    
def sendMessagesLimitedThread(queue, limit, responses):
    outstanding = [ ]
    debug = LOG.isEnabledFor(logging.DEBUG)
    while len(queue) > 0 or len(outstanding) > 0:
        while len(queue) > 0 and len(outstanding) < limit:
            msg, data, channel = queue.pop(0)
            if debug: LOG.debug("Sending %s to %s" % (str(data), str(channel)))
            sendMessage(msg, data, channel)
            outstanding.append(channel)
        # WHILE
        while len(outstanding) > 0:
            channel = outstanding.pop(0)
            msg = getMessage(channel.receive())
            responses.append(msg)
            break
        ## WHILE
        if debug: LOG.debug("Queue:%d / Outstanding:%d / Responses:%d" % \
                            (len(queue), len(outstanding), len(responses)))
    # WHILE

## DEF    

def sendMessage(msg, data, channel):
    '''serialize the data and send the msg through channel'''
    m = Message(msg, data)
    p = pickle.dumps(m, -1)
    channel.send(p)
    
def getMessage(item):
    ''' restore Message from channel'''
    return pickle.loads(item)
    
def getMessageName(msg):
    '''Return the name of the given message id'''
    return MSG_NAME_MAPPING[msg]

    
class Message:
    def __init__(self, header=MSG_NOOP, data=None):
        self.header = header
        self.data = data
## CLASS
