# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012
# Andy Pavlo - http://www.cs.brown.edu/~pavlo/
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

from datetime import datetime

#DB_NAME = 'microblog'
ARTICLE_COLL = 'articles'
COMMENT_COLL = 'comments'

NUM_AUTHORS = 1024
NUM_TAGS = 6000
NUM_TAGS_PER_ARTICLE = 40

ARTICLE_TITLE_SIZE = 200
ARTICLE_CONTENT_SIZE = 4096
COMMENT_CONTENT_SIZE = 512
MAX_COMMENT_RATING = 100
NUM_ARTICLES = 10000 # this is multiplied by the scale factor
NUMBER_OF_DATE_SUBRANGES = 8 # this breaks the interval between START_DATE and STOP_DATE in X segments

# Special atomic counter
NEXT_ARTICLE_CTR_ID = -9999
NEXT_ARTICLE_CTR_KEY = "nextArticleId"

#deprecated
#AUTHOR_NAME_SIZE = 20
#MAX_AUTHOR_SIZE = 20
#MAX_TITLE_SIZE = 200
#MAX_CONTENT_SIZE = 102400
#MAX_COMMENT_SIZE = 1024
#MAX_NUM_COMMENTS = 100




WORKLOAD_READ_PERCENT  = 90 
WORKLOAD_WRITE_PERCENT = 10 
assert (WORKLOAD_READ_PERCENT+WORKLOAD_WRITE_PERCENT) == 100

START_DATE = datetime.strptime('11/1/2011 1:30 PM', '%m/%d/%Y %I:%M %p')
STOP_DATE = datetime.strptime('1/1/2012 1:30 PM', '%m/%d/%Y %I:%M %p')

# Experiment Type Codes
EXP_SHARDING        = "sharding"
EXP_DENORMALIZATION = "denormalization"
EXP_INDEXING        = "indexing"
EXP_ALL = [ EXP_SHARDING, EXP_DENORMALIZATION, EXP_INDEXING ]

# Sharding Config Types
SHARDEXP_SINGLE     = 0
SHARDEXP_COMPOUND   = 1
SHARDEXP_ALL = [SHARDEXP_SINGLE, SHARDEXP_COMPOUND]

# Indexing Config Types
	
INDEXEXP_8020  = 0 # 80% reads / 20% writes	  	
INDEXEXP_9010  = 1 # 90% reads / 10% writes
INDEXEXP_ALL = [INDEXEXP_8020, INDEXEXP_9010]
