#!/usr/bin/python
import sys
import os
import string
import re
import logging
import traceback
import pymongo
import constants
from util import *
from pprint import pprint, pformat


# quick and dirty

def test():
	LOG = logging.getLogger(__name__)
	conn = None
	targetHost = "bronze.cs.brown.edu"
	targetPort = 27017
	try:
		conn = pymongo.Connection(targetHost, targetPort)
	except:
		LOG.error("Failed to connect to target MongoDB at %s:%s" % (targetHost, targetPort))
		raise
	#assert conn
	db = conn["test"]
	titleSize = 150
	contentSize = 6000
	numComments = 23
	articleId = 1
	articleDate = randomDate(constants.START_DATE, constants.STOP_DATE)
	title = randomString(titleSize)
	slug = list(title.replace(" ", ""))
	if len(slug) > 64: slug = slug[:64]
	for idx in xrange(0, len(slug)):
		if random.randint(0, 10) == 0:
			slug[idx] = "-"
				## FOR
	slug = "".join(slug)
	article = {
		"id": articleId,
		"title": title,
		"date": articleDate,
		"author": 1,
		"slug": slug,
		"content": randomString(contentSize),
		"numComments": numComments,
	}
	db[constants.ARTICLE_COLL].insert(article)
	print("perasa");
	commentCtr=0
	lastDate = articleDate
	for ii in xrange(0, numComments):
		lastDate = randomDate(lastDate, constants.STOP_DATE)
		commentAuthor = randomString(15)
		commentSize = 300
		commentContent = randomString(commentSize)
		
		comment = {
			"id": commentCtr,
			"article": articleId,
			"date": lastDate, 
			"author": commentAuthor,
			"comment": commentContent,
			"rating": 100
		}
		commentCtr += 1
		db[constants.ARTICLE_COLL].update({"id": articleId},{"$push":{"comments":comment}})
		if commentCtr==0 or commentCtr%1000==0:
			print(commentCtr)
# def			
if __name__ == '__main__':
	#executed as script
	# do something
	test()	
