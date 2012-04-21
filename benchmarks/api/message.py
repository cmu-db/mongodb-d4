# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Yang Lu
# http://www.cs.brown.edu/~yanglu/
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

import execnet
import pickle

MSG_EMPTY             = 0
MSG_CMD_INIT          = 1
MSG_CMD_LOAD          = 2
MSG_CMD_EXECUTE       = 3
MSG_CMD_STOP          = 4
MSG_INIT_COMPLETED    = 5
MSG_LOAD_COMPLETED    = 6
MSG_EXECUTE_COMPLETED = 7
MSG_CONFIG            = 8

NAME_MAPPING = { }
for key in globals().keys():
    if key.startswith("MSG_"):
        val = globals()[key]
        NAME_MAPPING[val] = key
## FOR

def sendMessage(msg, data, channel):
    '''serialize the data and send the msg through channel'''
    m = Message(msg,data)
    p = pickle.dumps(m, -1)
    channel.send(p)
    
def getMessage(item):
    ''' restore Message from channel'''
    return pickle.loads(item)
    
def getMessageName(msg):
    '''Return the name of the given message id'''
    return NAME_MAPPING[msg]

    
class Message:
    def __init__(self, header=MSG_EMPTY, data=None):
        self.header = header
        self.data = data
## CLASS
