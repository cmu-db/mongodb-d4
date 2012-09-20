"""
    This is lrubuffer is intended to be faster version of the existing lrubuffer.
    In this version, the buffer will be implemented as a dictionary which maps the current key
    to its predecessor's and successor's key:

    HEAD: <buffer-tuple>
    TAIL: <buffer-tuple>
    BUFFER: { <buffer-tuple>: [ <prev-buffer-tuple>, <next-buffer-tuple>, <size> ] }

    The HEAD and TAIL are used to pop and push entries out and into the buffer based on LRU rules
"""
import logging
import random
from pprint import pformat
from util import constants

# constants
PREV_BUFFER_TUPLE = 0
NEXT_BUFFER_TUPLE = 1
BUFFER_TUPLE_SIZE = 2
DOC_TYPE_INDEX = 0
DOC_TYPE_COLLECTION = 1

LOG = logging.getLogger(__name__)

class FastLRUBuffer:

    def __init__(self, collections, buffer_size, preload=constants.DEFAULT_LRU_PRELOAD):

        self.debug = LOG.isEnabledFor(logging.DEBUG)
        self.preload = preload

        self.collections = collections
        self.document_sizes = { } # size of each document in each collection: collection -> document size
        self.index_sizes = { } # size of indexes for each collection: collection -> index key size

        self.buffer = { }
        self.head = None # the top element in the buffer
        self.tail = None # the bottom element in the buffer

        # This is the total amount of space available in this buffer (bytes)
        self.buffer_size = buffer_size
        # This is the amount of space that is unallocated in this buffer (bytes)
        self.remaining = buffer_size
        self.evicted = 0
        self.refreshed = 0

        self.address_size = constants.DEFAULT_ADDRESS_SIZE # FIXME
        ## DEF

    def reset(self):
        """
            Reset the internal buffer and "free" all of its used memory
        """
        self.remaining = self.buffer_size
        self.buffer = { }
        self.evicted = 0
        self.refreshed = 0
        self.document_sizes = { }
        self.index_sizes = { }
        self.head = None
        self.tail = None
    ## DEF

    def initialize(self, design, delta):
        """
            Add the given collection to our buffer.
            This will automatically initialize any indexes for the collection as well.
        """
        self.reset()

        self.__init_index_and_document_size__(design)
        self.__init_collections__(design, delta)

    def getDocumentFromIndex(self, col_name, indexKeys, documentId):
        """
            Get the documents from the given index
            Returns the number of page hits incurred to read these documents.
        """
        size = self.index_sizes[col_name].get(indexKeys, 0)
        assert size > 0,\
        "Missing index size for %s -> '%s'\n%s" % (col_name, indexKeys, pformat(self.index_sizes))
        return self.getDocument(DOC_TYPE_INDEX, col_name, indexKeys, size, documentId)
        ## DEF

    def getDocumentFromCollection(self, col_name, documentId):
        """
            Get the documents from the given index
            Returns the number of page hits incurred to read these documents.
        """
        size = self.document_sizes.get(col_name, 0)

        assert size > 0, "Missing collection size for '%s'" % col_name

        return self.getDocument(DOC_TYPE_COLLECTION, col_name, col_name, size, documentId)
    ## DEF

    def getDocument(self, typeId, col_name, key, size, documentId):

        # Pre-hashing the tuple greatly improves the performance of this
        # method because we don't need to keep redoing it when we update
        buffer_tuple = self.__computeTupleHash__(typeId, key, documentId)

        if not buffer_tuple in self.buffer:
            offset = None
        else:
            LOG.debug("Looking up %d from buffer", buffer_tuple)
            try:
                offset = self.buffer[buffer_tuple]
            except ValueError:
                offset = None

        # The tuple is in our buffer, so we don't need to fetch anything from disk
        # We will need to push the tuple back on to the end of our buffer list
        if not offset is None:
            self.__update__(buffer_tuple)
            self.refreshed += 1
            return 0 # page_hits

        # It's not in the buffer for this index, so we're going to have
        # go fetch it from disk. Check whether we can just fetch
        # the page in or whether we will need to write out a dirty page right now
        # NOTE: We actually don't know whether the page is dirty. If it wasn't then
        #       OS would just discard it rather than write it back to disk. Figuring
        #       that out would be too expensive for this estimate, since we don't
        #       actually know how many documents are in that page that it needs to write
        else:
            assert not buffer_tuple in self.buffer, "Duplicate entry '%s'" % buffer_tuple
            assert size < self.buffer_size
            return self.__push__(buffer_tuple, size)
        ## DEF

    def __init_index_and_document_size__(self, design):

        for col_name in design.getCollections():
            col_info = self.collections[col_name]
            self.document_sizes[col_name] = max(col_info['avg_doc_size'], constants.DEFAULT_PAGE_SIZE)

            # For each collection, get the indexes that are included in the design.
            col_index_sizes = { }
            for index_keys in design.getIndexes(col_name):
                col_index_sizes[index_keys] = max(self.__getIndexSize__(col_info, index_keys), constants.DEFAULT_PAGE_SIZE)

            self.index_sizes[col_name] = col_index_sizes

    def __init_collections__(self, design, delta):

        for col_name in design.getCollections():
            if not self.preload: break

            col_info = self.collections[col_name]
            doc_size = self.document_sizes[col_name]

            # How much space are they allow to have in the initial configuration
            col_remaining = int(self.buffer_size * (delta * col_info['workload_percent']))
            if self.debug:
                LOG.debug("%s Pre-load Percentage: %.1f%% [bytes=%d]",
                    col_name, (delta * col_info['workload_percent'])*100, col_remaining)

            rng = random.Random()
            rng.seed(col_name)
            ctr = 0
            while (col_remaining - doc_size) > 0 and self.evicted == 0:
                documentId = rng.random()
                self.getDocumentFromCollection(col_name, documentId)
                col_remaining -= doc_size
                ctr += 1
                ## WHILE
            if self.debug:
                LOG.debug("Pre-loaded %d documents for %s - %s", ctr, col_name, self)
                ## FOR

        if self.debug and self.preload:
            LOG.debug("Total # of Pre-loaded Documents: %d", len(self.buffer.keys()))
        ## DEF

    ##  -----------------------------------------------------------------------
    ##  LRU operations
    ##  -----------------------------------------------------------------------

    def __pop__(self):
        """
            pop out the least recent used buffer_tuple from the buffer
        """
        self.__remove__(self.head)
        LOG.info("POP")
        self.evicted += 1
    ## DEF

    def __update__(self, buffer_tuple):
        """
            update the given buffer_tuple in the buffer
            Basically, it just puts the given buffer_tuple at the end of the queue
        """
        if buffer_tuple == self.tail:
            return

        isRemoved, value = self.__remove__(buffer_tuple)
        if isRemoved:
            LOG.debug("updating: tuple value %s: ", value)
            self.__push__(buffer_tuple, value[BUFFER_TUPLE_SIZE] if value[BUFFER_TUPLE_SIZE] else 0)

    ## DEF

    def __remove__(self, buffer_tuple):
        """
            remove a tuple from the buffer
        """
        if buffer_tuple is None or buffer_tuple not in self.buffer:
            return False, None
        else:
            tuple_value = self.buffer.pop(buffer_tuple)
            LOG.debug("Removing: tuple value %s: ", tuple_value)
            self.remaining += tuple_value[BUFFER_TUPLE_SIZE]

            # Reset Prev pointer
            if tuple_value[PREV_BUFFER_TUPLE] is None and tuple_value[NEXT_BUFFER_TUPLE] is None: # if only one tuple
                self.head = None
                self.tail = None
            elif tuple_value[PREV_BUFFER_TUPLE] is None: # if we remove the top tuple
                self.head = tuple_value[NEXT_BUFFER_TUPLE]
                self.buffer[self.head][PREV_BUFFER_TUPLE] = None # the second becomes the top now
            elif tuple_value[NEXT_BUFFER_TUPLE] is None: # if we remove the bottom tuple
                self.tail = tuple_value[PREV_BUFFER_TUPLE]
                self.buffer[self.tail][NEXT_BUFFER_TUPLE] = None # the last second becomes the bottom now
            else:
                self.buffer[tuple_value[PREV_BUFFER_TUPLE]][NEXT_BUFFER_TUPLE] = tuple_value[NEXT_BUFFER_TUPLE]
                self.buffer[tuple_value[NEXT_BUFFER_TUPLE]][PREV_BUFFER_TUPLE] = tuple_value[PREV_BUFFER_TUPLE]

            return True, tuple_value
    ## DEF

    def __push__(self, buffer_tuple, size):
        """
            Add the given buffer_tuple to the bottom of the buffer
        """
        LOG.debug("Adding")
        page_hits = 0
        if self.tail is None:
            self.head = buffer_tuple
            self.tail = buffer_tuple

            assert size <= self.buffer_size,\
            "This should not happen because the tuple size should be much smaller than buffer_size %s" % size
            self.buffer[buffer_tuple] = [None, None, size]
            self.remaining -= size
            page_hits += 1
        else:
            # pop out the least recent used tuples until we have enough space in buffer for this tuple
            while self.remaining < size:
                self.__pop__()
                page_hits += 1

            self.buffer[self.tail][NEXT_BUFFER_TUPLE] = buffer_tuple
            self.buffer[buffer_tuple] = [self.tail, None, size]
            self.tail = buffer_tuple
            self.remaining -= size
            page_hits += 1

        return page_hits

    ## DEF

    ## -----------------------------------------------------------------------
    ## UTILITY METHODS
    ## -----------------------------------------------------------------------

    def __computeTupleHash__(self, typeId, key, documentId):
        return hash((typeId, key, documentId))
        ## DEF

    def __getIndexSize__(self, col_info, indexKeys):
        """Estimate the amount of memory required by the indexes of a given design"""
        # TODO: This should be precomputed ahead of time. No need to do this
        #       over and over again.
        index_size = 0
        for f_name in indexKeys:
            f = col_info.getField(f_name)
            assert f, "Invalid index key '%s.%s'" % (col_info['name'], f_name)
            index_size += f['avg_size']
        index_size += self.address_size
        if self.debug: LOG.debug("%s Index %s Memory: %d bytes",\
            col_info['name'], repr(indexKeys), index_size)
        return index_size
        ## DEF

    def __str__(self):
        buffer_ratio = (self.buffer_size - self.remaining) / float(self.buffer_size)
        return "Buffer Usage %.2f%% [evicted=%d / refreshed=%d / entries=%d / used=%d / total=%d]" % (\
            buffer_ratio*100,
            self.evicted,
            self.refreshed,
            len(self.buffer.keys()),
            self.buffer_size - self.remaining,
            self.buffer_size,
            )
        ## DEF

    def validate(self):
        """Check that the buffer is in a valid state"""
        assert self.remaining >= 0,\
        "The buffer has a negative remaining space"
        assert self.remaining <= self.buffer_size,\
        "The buffer has more remaining space than the original buffer size"
        ## DEF
## CLASS