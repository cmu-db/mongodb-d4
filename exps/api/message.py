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

import execnet
import pickle

# All of the strings in this list will become
# status codes that are prefixed with "MSG_"
# The values of these codes will all be unique
MSG_STATUS_CODES = [
    "EMPTY",
    "CMD_INIT",
    "CMD_LOAD",
    "CMD_STATUS",
    "CMD_EXECUTE_INIT",
    "CMD_EXECUTE",
    "CMD_STOP",
    "INIT_COMPLETED",
    "LOAD_STATUS",
    "LOAD_COMPLETED",
    "EXECUTE_COMPLETED",
    "CONFIG"
]
MSG_NAME_MAPPING = { }
for code in xrange(0, len(MSG_STATUS_CODES)):
    name = "MSG_%s" % MSG_STATUS_CODES[code]
    globals()[name] = code
    MSG_NAME_MAPPING[code] = name
## FOR

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
    def __init__(self, header=MSG_EMPTY, data=None):
        self.header = header
        self.data = data
## CLASS
