"""
    This is lrubuffer is intended to be faster version of the existing lrubuffer.
    In this version, the buffer will be implemented as a dictionary which maps the current key
    to its predecessor's and successor's key:

    HEAD: <buffer-tuple>
    TAIL: <buffer-tuple>
    BUFFER: { <buffer-tuple>: [ <prev-buffer-value>, <next-buffer-value>, <hash-key> ] }

    The HEAD and TAIL are used to pop and push entries out and into the buffer based on LRU rules
    """
import logging
import random
from pprint import pformat

# constants
PREV_BUFFER_ENTRY = 0
NEXT_BUFFER_ENTRY = 1
HASH_KEY = 2

DOC_TYPE_INDEX = 0
DOC_TYPE_COLLECTION = 1

BUFFER = 0
HEAD = 1
TAIL = 2

LOG = logging.getLogger(__name__)

class FastLRUBufferWithWindow:

    def __init__(self, window_size):

        self.debug = False
        
        self.buffer = { }
        self.head = None # the top element in the buffer
        self.tail = None # the bottom element in the buffer

        # This is the total amount of slots available in this buffer (integer)
        self.window_size = window_size
        # This is the amount of space that is unallocated in this buffer (bytes)
        self.free_slots = window_size
        self.evicted = 0
        self.refreshed = 0

        # self.address_size = constants.DEFAULT_ADDRESS_SIZE # FIXME
        ## DEF

    def reset(self):
        """
            Reset the internal buffer and "free" all of its used memory
        """
        self.free_slots = self.window_size
        self.buffer = { }
        self.evicted = 0
        self.refreshed = 0
        self.head = None
        self.tail = None
    ## DEF

    def getDocumentFromIndex(self, indexKeys, documentId):
        """
            Get the documents from the given index
            Returns the number of page hits incurred to read these documents.
        """
#        assert size > 0,"Missing index size for %s -> '%s'\n%s" % (col_name, indexKeys, pformat(self.index_sizes))
        return self.getDocument(DOC_TYPE_INDEX, indexKeys, documentId)
        ## DEF

    def getDocumentFromCollection(self, col_name, documentId):
 #      assert size > 0, "Missing collection size for '%s'" % col_name
        return self.getDocument(DOC_TYPE_COLLECTION, col_name, documentId)
    ## DEF

    def getDocument(self, typeId, keys, documentId):

        # Pre-hashing the tuple greatly improves the performance of this
        # method because we don't need to keep redoing it when we update
        buffer_tuple = (documentId, keys, typeId)

        offset = self.buffer.get(buffer_tuple, None)

        # The tuple is in our buffer, so we don't need to fetch anything from disk
        # We will need to push the tuple back on to the end of our buffer list
        if offset:
            self.__update__(buffer_tuple)
            self.refreshed += 1
            if self.debug:
                self.__print_buffer__()
            return 0 # page_hits

        # It's not in the buffer for this index, so we're going to have
        # go fetch it from disk. Check whether we can just fetch
        # the page in or whether we will need to write out a dirty page right now
        # NOTE: We actually don't know whether the page is dirty. If it wasn't then
        #       OS would just discard it rather than write it back to disk. Figuring
        #       that out would be too expensive for this estimate, since we don't
        #       actually know how many documents are in that page that it needs to write
        else:
#            assert not buffer_tuple in self.buffer, "Duplicate entry '%s'" % buffer_tuple
#            assert size < self.buffer_size
            if self.debug:
                self.__print_buffer__()
            return self.__push__(buffer_tuple)
        ## DEF

    ##  -----------------------------------------------------------------------
    ##  LRU operations
    ##  -----------------------------------------------------------------------

    def __print_buffer__(self):
        """
            Print out the current entries in the buffer(most used to lease used)
        """
        counter = 0
        pointer = self.tail
        while pointer is not None:
            print "Entry " + str(counter) + ": " + str(pointer)
            counter += 1
            pointer = pointer[PREV_BUFFER_ENTRY]
        print "total number of entries: " + str(counter)
        
    def __pop__(self):
        """
            pop out the least recent used buffer_tuple from the buffer
        """
        self.__remove__(self.head[HASH_KEY])
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
            self.__push__(buffer_tuple)

    ## DEF

    def __remove__(self, buffer_tuple):
        """
            remove a tuple from the buffer
        """
        tuple_value = self.buffer.pop(buffer_tuple, None)
        if tuple_value is None:
            return False, None
            
        # Add back the memory to the buffer based on what we allocated for the tuple
        self.free_slots += 1

        if tuple_value[PREV_BUFFER_ENTRY] is None and tuple_value[NEXT_BUFFER_ENTRY] is None: # if only one tuple
            self.head = None
            self.tail = None
        elif tuple_value[PREV_BUFFER_ENTRY] is None: # if we remove the top tuple
            self.head = tuple_value[NEXT_BUFFER_ENTRY]
            self.head[PREV_BUFFER_ENTRY] = None # the second becomes the top now
        elif tuple_value[NEXT_BUFFER_ENTRY] is None: # if we remove the bottom tuple
            self.tail = tuple_value[PREV_BUFFER_ENTRY]
            self.tail[NEXT_BUFFER_ENTRY] = None
        else:
            prev = tuple_value[PREV_BUFFER_ENTRY]
            prev[NEXT_BUFFER_ENTRY] = tuple_value[NEXT_BUFFER_ENTRY]
            
            next = tuple_value[NEXT_BUFFER_ENTRY]
            next[PREV_BUFFER_ENTRY] = tuple_value[PREV_BUFFER_ENTRY]

        return True, tuple_value
    ## DEF

    def __push__(self, buffer_tuple):
        """
            Add the given buffer_tuple to the bottom of the buffer
        """
        page_hits = 0
        # pop out the least recent used tuples until we have enough space in buffer for this tuple
        while self.free_slots <= 0:
            self.__pop__()
            page_hits += 1
        
        newEntry = [self.tail, None, buffer_tuple]
        
        if self.tail is not None:
            self.tail[NEXT_BUFFER_ENTRY] = newEntry
        else:
            self.head = newEntry

        self.tail = newEntry
        self.buffer[buffer_tuple] = newEntry
        self.free_slots -= 1
        page_hits += 1

        return page_hits

    ## DEF

    ## -----------------------------------------------------------------------
    ## UTILITY METHODS
    ## -----------------------------------------------------------------------

    def __str__(self):
        buffer_ratio = (self.window_size - self.free_slots) / float(self.window_size)
        return "Buffer Usage %.2f%% [evicted=%d / refreshed=%d / entries=%d / used=%d / total=%d]" % (\
            buffer_ratio*100,
            self.evicted,
            self.refreshed,
            len(self.buffer.keys()),
            self.window_size - self.free_slots,
            self.window_size,
            )
        ## DEF

## CLASS