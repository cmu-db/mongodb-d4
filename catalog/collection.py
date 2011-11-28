# -*- coding: utf-8 -*-

import sys
import json
import logging
from common import *

## ==============================================
## Collection
## ==============================================
class Collection(object):
    def __init__(self, name, keys=[]):
        self.name = name
        self.keys = keys[:]