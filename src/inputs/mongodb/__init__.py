__author__ = 'pavlo'

# Third-Party Dependencies
import os, sys
basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../libs"))
sys.path.append(os.path.join(basedir, ".."))

from abstractconverter import AbstractConverter
from mongosniffconverter import MongoSniffConverter