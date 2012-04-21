#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Yang Lu
# http:##www.cs.brown.edu/~pavlo/
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

EMPTY=0
CMD_INIT=1
CMD_LOAD = 2
CMD_EXECUTE = 3
CMD_STOP = 4
INIT_COMPLETED = 5
LOAD_COMPLETED = 6
EXECUTE_COMPLETED = 7
CONFIG = 8

def sendMessage(msg,data,channel):
    '''serialize the data and send the msg through channel'''
    m = Message(msg,data)
    channel.send(pickle.dumps(m,-1))
    
def getMessage(item):
    ''' restore Message from channel'''
    return pickle.loads(item)
    
class Message:
    def __init__(self,header=EMPTY,data=None):
        self.header=header
	self.data=data
