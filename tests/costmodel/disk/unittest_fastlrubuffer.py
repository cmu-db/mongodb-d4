import os, sys
import unittest

basedir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, "../../../src"))

from costmodel.disk.fastlrubufferusingwindow import FastLRUBufferWithWindow

class TestFastLRUbufferWithWindow(unittest.TestCase):

    def setUp(self):
        pass
        
    def testAllBufferOperations_push(self):
        self.lru = FastLRUBufferWithWindow(1)
        slot_size = 1
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        
        self.assertEqual(len(self.lru.buffer), self.lru.window_size)
    
    def testAllBufferOperations_push_slotsize_0(self):
        self.lru = FastLRUBufferWithWindow(10)
        slot_size = 1
        for i in xrange(9):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        
        tup = (9)
        slot_size = 9
        self.lru.__push__(9, slot_size)
        self.assertEqual(len(self.lru.buffer), 2)
        
    def testAllBufferOperations_push_slotsize_1(self):
        self.lru = FastLRUBufferWithWindow(10)
        slot_size = 1
        for i in xrange(9):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        
        tup = (9)
        slot_size = 10
        self.lru.__push__(9, slot_size)
        self.assertEqual(len(self.lru.buffer), 1)
    ## DEF
    
    def testAllBufferOperations_push_slotsize_2(self):
        self.lru = FastLRUBufferWithWindow(10)
        slot_size = 1
        for i in xrange(9):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        
        tup = (9)
        slot_size = 10
        self.lru.__push__(tup, slot_size)
        self.assertEqual(len(self.lru.buffer), 1)
        
        slot_size = 1
        for i in xrange(9):
            tup = (i)
            self.lru.__push__(tup, slot_size)
            
        self.assertEqual(len(self.lru.buffer), 9)
    ## DEF
    
    def testAllBufferOperations_push_slotsize_3(self):
        self.lru = FastLRUBufferWithWindow(10)
        slot_size = 1
        for i in xrange(9):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        
        tup = (9)
        slot_size = 10
        self.lru.__push__(tup, slot_size)
        self.assertEqual(len(self.lru.buffer), 1)
        
        tup = (11)
        slot_size = 1
        self.lru.__push__(tup, slot_size)
            
        self.assertEqual(len(self.lru.buffer), 1)
    ## DEF
    
    def testAllBufferOperations_update(self):
        self.lru = FastLRUBufferWithWindow(100)
        slot_size = 1
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup, slot_size)
            
        for i in xrange(100):
            tup = (i)
            self.lru.__update__(tup)
            self.assertEqual(self.lru.tail[2], i)
            
    def testAllBufferOperations_pop(self):
        self.lru = FastLRUBufferWithWindow(100)
        slot_size = 1
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup, slot_size)
        for i in xrange(100):
            self.lru.__pop__()
            self.assertEqual(len(self.lru.buffer), self.lru.window_size - i - 1)


if __name__ == '__main__':
    unittest.main()


