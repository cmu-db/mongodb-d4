#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import fileinput
import hashlib
import time
import re
import argparse

def is_string(w):
    try:
        float(w)
        return False
    except ValueError:
        return True

def is_sensitive(w):
    return w.startswith("\"") and (w.endswith("\"") or w.endswith("\","))

def hash_string(w, salt):
    orig_len = len(w)
    has_comma = w.endswith(",")
    w = w[1:]
    if has_comma:
        w = w[:-2]
        comma = ","
    else:
        w = w[:-1]
        comma = ""
        
    # hash/length <optional comma>
    return "%s/%d%s" % (hashlib.md5(str(salt) + w).hexdigest(), orig_len, comma)

def sanitize(line, salt):
    words = line.split()
    new_words = []
    for w in words:
        if is_sensitive(w):
            new_words.append(hash_string(w, salt))
        else:
            new_words.append(w)

    sanitized_line = " ".join(new_words)
    return sanitized_line;

#this selects only lines we care about - starting with query
def is_important(line):
    l = line.lstrip("\t ") #tab and space
    return l.startswith("query")

if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='MongoSniff Trace Anonymizer')
    aparser.add_argument('salt', type=int,
                         help='Random hash salt')
    args = vars(aparser.parse_args())
    
    #for line in sys.stdin:
        #if is_important(line):
        #    print timestamp(sanitize(line))
     #   print line,
     
    newCommand = re.compile("^(.*?) (\-\->>|<<\-\-) (.*?)")
    line = sys.stdin.readline()
    while line:
        line = sanitize(line, args['salt'])
        
        # Check whether this is a new command
        # If it is, then this is only line that should get a timestamp
        if newCommand.match(line):
            timestamp = repr(time.time()) + " -"
        else:
            timestamp = "  "
        print "%-20s %s" % (timestamp, line)

        line = sys.stdin.readline()
## MAIN        




    
