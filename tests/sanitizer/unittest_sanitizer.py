#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import unittest
from sanitizer import anonymize


def get_long_string(strings, iters):
    str = ""
    for i in range(iters):
        for s in strings:
            str += s
    #print str
    return str



class TestSanitizer (unittest.TestCase):
    
    def setUp(self):
        self.s = anonymize.Sanitizer(None, None, True)
        pass

    def testHashStringSimple(self):
        # other tests
        str1 = "\"THIS SHOULD BE SIMPLY HASHED\""
        hash1 = anonymize.hash_string("THIS SHOULD BE SIMPLY HASHED", 0, True)
        result1 = self.s.sanitize(str1, 0)
        self.assertEqual(hash1, result1)

    def testHashMultiline(self):
        # short strings
        short_string1 = get_long_string([' \\"hello\\" ', '\n\n \t\t \n\t \t\n', 'email: \\"emanuel@buzek.net\\"', '\'\'\'\'', "'\\\"'", "{inside: \\\"of a string\\\"}"], 1)
        short_string2 = get_long_string(['This is not a long string.  \n\n \t', '\n\t', '{\\"name\\"}', ' \\ END', '\\"string\\"', "'string'", "\\\"'quotes'\\\"", "open quotes: \\\" '"], 1)
        
        hash1 = anonymize.hash_string(short_string1, 0, True)
        hash2 = anonymize.hash_string(short_string2, 0, True)
        
        json = '{"key1" : "%s", "key2" : "%s"}' % (short_string1, short_string2)
#        print json
        
        expected_sanitized_json = '{"key1" : %s, "key2" : %s}' % (hash1, hash2)
        real_sanitized_json = self.s.sanitize(json, 0)
        
        self.assertEqual(expected_sanitized_json, real_sanitized_json)

    def testHashLongStrings(self):
        # very very long strings
        long_string1 = get_long_string([' \\"hello\\" ', '\n\n \t\t \n\t \t\n', 'email: \\"emanuel@buzek.net\\"', '\'\'\'\'', "'\\\"'", "{inside: \\\"of a string\\\"}"], 5000)
        long_string2 = get_long_string(['This is a very string.  \n\n \t', '\n\t', '{\\"name\\"}', ' \\ END', '\\"string\\"', "'string'", "\\\"'quotes'\\\"", "open quotes: \\\" '"], 5000)
        
        hash1 = anonymize.hash_string(long_string1, 0, True)
        hash2 = anonymize.hash_string(long_string2, 0, True)
        
        long_json = '{"key1" : "%s", "key2" : "%s"}' % (long_string1, long_string2)
        
        expected_sanitized_long_json = '{"key1" : %s, "key2" : %s}' % (hash1, hash2)
        real_sanitized_long_json = self.s.sanitize(long_json, 0)
        
        self.assertEqual(expected_sanitized_long_json, real_sanitized_long_json)
    ## DEF
        

    def testHashStringMany(self):
        # many strings in json
        s = anonymize.Sanitizer(None, None, True)
        text = 'string with \\\"escaped quotes\\\"'
        hashed_text = anonymize.hash_string(text, 0, True)
        long_json = "{" + get_long_string(['"key" : "%s", ' % text], 4000) + "}"
        expected_result = "{" + get_long_string(['"key" : %s, ' % hashed_text], 4000) + "}"
        real_result = self.s.sanitize(long_json, 0)
        
        #print long_json
        #print expected_result
        #print real_result
        
        self.assertEqual(expected_result, real_result)
        
    ## DEF

    def testTrace(self):
        
        # This test loads two traces - a clean, unhashed trace form the sniffer, and the anonymized trace
        # The clean trace is then anonymized and compared to the expected output.
        # This verifies that the sanitizer works as expected.
        
        basedir = os.path.dirname(os.path.realpath(__file__))

        traceClean = os.path.join(basedir, "trace-clean.out")
        clean_lines = open(traceClean, "r").readlines()
        
        traceAnon = os.path.join(basedir, "trace-anon.out")
        anon_lines = open(traceAnon, "r").readlines()

        s = anonymize.Sanitizer(None, None, True) # test: True

        for i in range(len(clean_lines)):
            source = clean_lines[i]
            expected = anon_lines[i]
            result = self.s.process_line(source) + "\n"
            #print result
            #print expected
            self.assertEqual(expected, result)

        print "done"

## CLASS

if __name__ == '__main__':
    unittest.main()
## MAIN