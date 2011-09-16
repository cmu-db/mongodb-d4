#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time


def is_string(w):
    try:
        float(w)
        return False
    except ValueError:
        return True

def is_sensitive(w):
    return w.startswith("\"") and (w.endswith("\"") or w.endswith("\","))


def hash_string(w):
    has_comma = w.endswith(",")
    w = w[1:]
    if has_comma:
        w = w[:-2]
    else:
        w = w[:-1]
    hashed = hashlib.md5(w).hexdigest()
    hashed = "\""+ hashed + "\""
    if has_comma:
        hashed = hashed + ","
    return hashed

def sanitize(line):
    words = line.split()
    new_words = []
    for w in words:
        if is_sensitive(w):
            new_words.append(hash_string(w))
        else:
            new_words.append(w)

    sanitized_line = " ".join(new_words)
    return sanitized_line;

#this selects only lines we care about - starting with query
def is_important(line):
    l = line.lstrip("\t ") #tab and space
    return l.startswith("query")

def timestamp(line):
    stamp = time.time()
    return line + " TIMESTAMP: " + repr(stamp)

def main():
    #for line in sys.stdin:
        #if is_important(line):
        #    print timestamp(sanitize(line))
     #   print line,
    line = sys.stdin.readline()
    while line:
        #if is_important(line): #check it starts with query...
        print timestamp(sanitize(line)) 
        line = sys.stdin.readline()
        
    return
        

if __name__ == '__main__':
	main()



    
