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
class MaxNum(self):
	def test(self):
		conn = None
		targetHost = "bronze.cs.brown.edu"
		targetPort = "27017"
		if self.debug:
			LOG.debug("Connecting MongoDB database at %s:%d" % (targetHost, targetPort))
		try:
			conn = pymongo.Connection(targetHost, targetPort)
		except:
			LOG.error("Failed to connect to target MongoDB at %s:%s" % (targetHost, targetPort))
			raise
		assert conn
		db = self.conn["test"]
		print("test");
		titleSize = 200
		contentSize = 4000
		numComments = 2
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
			"id": 1,
			"title": title,
			"date": articleDate,
			"author": "1",
			"slug": slug,
			"content": randomString(contentSize),
			"numComments": numComments,
		}

		lastDate = articleDate
		for ii in xrange(0, numComments):
			lastDate = randomDate(lastDate, constants.STOP_DATE)
			commentAuthor = randomString(int(random.gauss(constants.MAX_AUTHOR_SIZE/2, constants.MAX_AUTHOR_SIZE/4)))
			commentContent = randomString(int(random.gauss(constants.MAX_COMMENT_SIZE/2, constants.MAX_COMMENT_SIZE/4)))
			
			comment = {
				"id": self.getNextCommentId(),
				"article": articleId,
				"date": lastDate, 
				"author": commentAuthor,
				"comment": commentContent,
				"rating": int(self.ratingZipf.next())
			}
			commentCtr += 1
			self.db[constants.ARTICLE_COLL].update({"id": articleId},{"$push":{"comments":comment}})
			if articleId==0 or articleId%1000:
				print "Inserted ".commentCtr." articles so far\n"
				
	if __name__ == '__main__':
		#executed as script
		# do something
		self.test()	