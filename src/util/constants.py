# -*- coding: utf-8 -*-
import sys

PROJECT_NAME = "mongodb-d4"
PROJECT_URL = "https://github.com/apavlo/mongodb-d4"

## ==============================================
## METADATA DB
## ==============================================

# The default name of the metadata database
METADATA_DB_NAME = "metadata"

# The schema catalog information about the application
COLLECTION_SCHEMA = "schema"
COLLECTION_WORKLOAD = "sessions"

CATALOG_COLL = "catalog"
CATALOG_FIELDS = 'fields'

## ==============================================
## DATASET DB
## ==============================================

# The default name of the reconstructed database
DATASET_DB_NAME = "dataset"

## ==============================================
## WORKLOAD PROCESSING OPTIONS
## ==============================================

SKIP_MONGODB_ID_FIELD = False

# List of collection names prefixes that we should ignore
# when performing various processing tasks
IGNORED_COLLECTIONS = [ 'system', 'local', 'admin', 'config' ]

# If a query's collection name is mangled when processing traces,
# we'll use this value to indicate that it is invalid
INVALID_COLLECTION_MARKER = "*INVALID*"

# The default initial session id. New session ids will
# start at this value
INITIAL_SESSION_ID = 100

# Special marker that represents a 'virtual' field for the
# inner values of a list type
LIST_INNER_FIELD = "__INNER__"

# Replace any key that starts with a '$' with this string
REPLACE_KEY_DOLLAR_PREFIX = '#'

# Replace any '.' in a key with this string
REPLACE_KEY_PERIOD = '__'

# This identifies that an operation has to perform a full scan
# on an entire collection rather than retrieving a single document
FULL_SCAN_DOCUMENT_ID = sys.maxint

## ==============================================
## MONGO OPERATION TYPES
## ==============================================
OP_TYPE_QUERY = '$query'
OP_TYPE_INSERT = '$insert'
OP_TYPE_ISERT = '$isert'
OP_TYPE_DELETE = '$delete'
OP_TYPE_UPDATE = '$update'
OP_TYPE_REPLY = '$reply'
OP_TYPE_GETMORE = '$getMore'
OP_TYPE_KILLCURSORS = '$killCursors'
OP_TYPE_UNKNOWN = 'unknown'
OP_TYPE_ALL = [ ]
for k in locals().keys():
    if k.startswith("OP_TYPE_"):  OP_TYPE_ALL.append(locals()[k])

## ==============================================
## PREDICATE TYPES
## ==============================================
PRED_TYPE_RANGE     = 'range'
PRED_TYPE_EQUALITY  = 'eq'
PRED_TYPE_REGEX     = 'regex'

## ==============================================
## COSTMODEL DEFAULTS
## ==============================================
DEFAULT_ADDRESS_SIZE = 8 # bytes
DEFAULT_TIME_INTERVALS = 10

# Whether to preload documents in the LRUBuffers
DEFAULT_LRU_PRELOAD = True

# The size of pages on disk for each MongoDB database node
DEFAULT_PAGE_SIZE = 4096 # bytes

# Window size in lru buffer: how many collection are preloaded into the buffer
WINDOW_SIZE = 1024

# Slot size upper bound: if the slot size is larger than this value, we will consider it as a 
# full page scan
SLOT_SIZE_LIMIT = 10

## ==============================================
## CANDIDATES GENERATOR CONSTRAINTS
## ==============================================
MIN_SELECTIVITY = 0.25

MAX_INDEX_SIZE = 10

EXAUSTED_SEARCH_BAR = 4

NUMBER_OF_BACKUP_KEYS = 2

## ==============================================
## MONGO DATASET RECONSTRUCTION CONSTRAINTS
## ==============================================

# The minimum size of nested fields, with which we will extract them from its parent collection
MIN_SIZE_OF_NESTED_FIELDS = 3

# Split documents with more than K fields
MIN_SPLIT_SIZE = 3

# We want to SKIP these two fields since we are functional fields not data fields
FUNCTIONAL_FIELD = 'parent_col'