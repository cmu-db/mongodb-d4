# -*- coding: utf-8 -*-

import sys
import json
import logging
from common import *

## ==============================================
## Key
## ==============================================
class Key(object):

    def __init__(self, name, type):
        self.name = name
        self.type = type
        
    