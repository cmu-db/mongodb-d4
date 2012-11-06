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
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup)
        
        self.assertEqual(len(self.lru.buffer), self.lru.window_size)
        
    def testAllBufferOperations_update(self):
        self.lru = FastLRUBufferWithWindow(100)
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup)
            
        for i in xrange(100):
            tup = (i)
            self.lru.__update__(tup)
            self.assertEqual(self.lru.tail[2], i)
            
    def testAllBufferOperations_pop(self):
        self.lru = FastLRUBufferWithWindow(100)
        
        for i in xrange(100):
            tup = (i)
            self.lru.__push__(tup)
        for i in xrange(100):
            self.lru.__pop__()
            self.assertEqual(len(self.lru.buffer), self.lru.window_size - i - 1)


if __name__ == '__main__':
    unittest.main()


