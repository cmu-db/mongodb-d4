# -*- coding: utf-8 -*-

PROJECT_NAME = "MongoDB-Designer"
PROJECT_URL = "https://github.com/apavlo/MongoDB-Designer"

# Metadata DB Collections
COLLECTION_SCHEMA = "schema"
COLLECTION_WORKLOAD = "sessions"

CATALOG_COLL = "catalog"
CATALOG_FIELDS = 'fields'

WORKLOAD_SESSIONS = 'sessions'

# List of collection names prefixes that we should ignore
# when performing various processing tasks
IGNORED_COLLECTIONS = [ 'system', 'local', 'admin' ]

# If a query's collection name is mangled when processing traces,
# we'll use this value to indicate that it is invalid
INVALID_COLLECTION_MARKER = "*INVALID*"

# The default initial session id. New session ids will
# start at this value
INITIAL_SESSION_UID = 100

# Special marker that represents a 'virtual' field for the
# inner values of a list type
LIST_INNER_FIELD = "__INNER__"

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