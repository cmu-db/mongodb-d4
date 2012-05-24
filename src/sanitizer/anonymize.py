#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import fileinput
import hashlib
import time
import re
import optparse

def find_quote(line, startIndex):
    index = startIndex
    while index < len(line):
        index = line.find("\"", index)
        if index < 0:
            return (-1, False)
        if (index == 0) or (line[index-1] != "\\"):
            # found an unescaped quote
            break
        # advance after the current "
        index += 1
    isKey = False
    if len(line)-index > 2:
        isKey = line[index + 2] == ":"
    return (index, isKey)

def hash_string(s, salt):
    return "%s/%d" % (hashlib.md5(str(salt) + s).hexdigest(), len(s))

def sanitize(line, salt):
    startIndex = 0
    endIndex = 0
    resultLine = ""
    #print line
    
    # go through the line, and find unescaped quotes
    # replace strings with their hash value
    while endIndex < len(line):
        # find start of a string
        (startIndex, flag) = find_quote(line, endIndex)
        if startIndex < 0:
            break
        
        # store the substring between the last quote and the current one    
        b = line[endIndex: startIndex]
        
        # find the end of the string
        (endIndex, isKey) = find_quote(line, startIndex + 1)
        if endIndex < 0:
            #print "ERROR: open string: ", line
            # on error, just don't sanitize...
            return line
        endIndex += 1
        
        # this is the substring we want to hash
        string = line[startIndex: endIndex]
        #print "found string: ", string
        if not isKey:
            string = hash_string(string[1:len(string)-1], salt) #strip surrounding quotes
        # append to the result...
        resultLine = resultLine + b + string
    ### END WHILE
    
    # append the rest of the line
    if endIndex < len(line):
        resultLine = resultLine + line[endIndex: len(line)]
    #print "RESULT:", resultLine
    return resultLine.rstrip("\n")

#this selects only lines we care about - starting with query
def is_important(line):
    l = line.lstrip("\t ") #tab and space
    return l.startswith("query")

if __name__ == '__main__':
    aparser = optparse.OptionParser(description='MongoSniff Trace Anonymizer')
    aparser.add_option('-s', '--salt', dest='salt', type=int,
                         help='Random hash salt')
    aparser.add_option('-o', '--out', dest='out', type=str,
                         help='output file name')
    (options, args) = aparser.parse_args()
    
    f = None 
    if options.out: 
        f = open(options.out, 'w')

    newCommand = re.compile("^(.*?) (\-\->>|<<\-\-) (.*?)")
    line = sys.stdin.readline()
    while line:
        line = sanitize(line, options.salt)
        
        # Check whether this is a new command
        # If it is, then this is only line that should get a timestamp
        if newCommand.match(line):
            timestamp = repr(time.time()) + " -"
        else:
            timestamp = "  "
        
        output =  "%-20s %s" % (timestamp, line)
        print output
        if f:
            f.write(output)
            f.write("\n")
        line = sys.stdin.readline()

    f.close()
## MAIN        




    
