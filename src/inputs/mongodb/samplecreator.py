#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time
import re
import argparse
import yaml
import json
import logging
from pymongo import Connection
import random
import string

sys.path.append("../workload")
from traces import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

### DEFAULT VALUES
### you can specify these with args
TARGET_DB = "sample_db"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = "27017"

#GLOBAL vars
target_db = None
connection = None





def initDB(hostname, port, t_db):
    global connection
    global target_db

    # Initialize connection to db that stores raw transactions
    connection = Connection(hostname, port)
    target_db = connection[t_db]
    
    return

def getRandomString(l):
    return "".join(random.sample(string.letters+string.digits, l))


def getRandomUser():
    return {"first": getRandomString(8), "last": getRandomString(8), "address": {"street": getRandomString(8), "list": [getRandomString(2), getRandomString(2), getRandomString(2)]}}

def getRandomArticle():
    return {"Title": getRandomString(20), "author": getRandomString(8), "text": getRandomString(30)}

def populate():
    #sanity check
    users = []
    users.append({"first": "Emanuel", "last": "Buzek", "address": {"street": "Wix", "list": ["a", "b", "c"]}})
    users.append({"first": "Andy", "last": "Pavlo", "address": {"street": "Brown", "list": ["1", "2", "3"]}})
    users.append({"first": "Delete_me", "last": "XXX", "address": {"street": "homeless", "list": ["1", "2", "3"]}})
    #add a bunch of other users...
    for i in range(20):
        users.append(getRandomUser())
    target_db.users.insert(users)
    
    
    articles = []
    articles.append({"Title": "Why We Should Ban Religion And Kill The Pope", "author": "Buzek", "text": "Read online on www.fuckreligion.org"})
    articles.append({"Title": "Blah blah blah", "author": "Pavlo", "text": "Database bullshit"})
    for i in range(5):
        articles.append(getRandomArticle())
    target_db.articles.insert(articles)
    
    print("Done.")




def clear():
    target_db.users.remove()
    target_db.articles.remove()
    

def test():
    populate()
    
    target_db.users.find_one()
    
    #get the count of all articles
    target_db.articles.find().count()
    
    #delete one article
    target_db.users.remove({'first': 'Delete_me'})
    
    #update
    target_db.users.update({'last': 'Buzek'}, {'first': 'Ema'}, True, True)
    
     #retrieve all articles
    target_db.articles.find()

def main():    
    aparser = argparse.ArgumentParser(description='Sample Creator')
    aparser.add_argument('--host',
                         help='hostname of machine running mongo server', default=DEFAULT_HOST)
    aparser.add_argument('--port', type=int,
                         help='port to connect to', default=DEFAULT_PORT)
    aparser.add_argument('--target_db', help='db for the sample data', default=TARGET_DB)
     
    args = vars(aparser.parse_args())

    LOG.info("..:: Sample Creator ::..")

    settings = "host: ", args['host'], " port: ", args['port'], " target_db: ", args['target_db'] 
    LOG.info(settings)

    initDB(args['host'], args['port'], args['target_db'])

    clear()

    test()
    

    return
    
if __name__ == '__main__':
    main()



    
