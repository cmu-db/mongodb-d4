#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import fileinput
import hashlib
import time
import re
import optparse


class Sanitizer:
    

    def find_quote(self, line, startIndex):
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
    
    def hash_string(self, s, salt):
        #print s
        hash = hashlib.md5(str(salt) + s).hexdigest()
        if self.test:
            hash = "XXX_HASH_XXX"
        output = "%s/%d" % (hash, len(s))
        return output
        
    def sanitize(self, line, salt):
        startIndex = 0
        endIndex = 0
        resultLine = ""
        #print line
        
        # go through the line, and find unescaped quotes
        # replace strings with their hash value
        while endIndex < len(line):
            # find start of a string
            (startIndex, flag) = self.find_quote(line, endIndex)
            if startIndex < 0:
                break
            
            # store the substring between the last quote and the current one    
            b = line[endIndex: startIndex]
            
            # find the end of the string
            (endIndex, isKey) = self.find_quote(line, startIndex + 1)
            if endIndex < 0:
                #print "ERROR: open string: ", line
                # on error, just don't sanitize...
                return line
            endIndex += 1
            
            # this is the substring we want to hash
            string = line[startIndex: endIndex]
            #print "found string: ", string
            if not isKey:
                string = self.hash_string(string[1:len(string)-1], salt) #strip surrounding quotes
            # append to the result...
            resultLine = resultLine + b + string
        ### END WHILE
        
        # append the rest of the line
        if endIndex < len(line):
            resultLine = resultLine + line[endIndex: len(line)]
        #print "RESULT:", resultLine
        return resultLine.rstrip("\n")
    
    #this selects only lines we care about - starting with query
    def is_important(self, line):
        l = line.lstrip("\t ") #tab and space
        return l.startswith("query")
    
    
    def process_line(self, line):
        line = self.sanitize(line, self.salt)
            
        # Check whether this is a new command
        # If it is, then this is only line that should get a timestamp
        if self.new_command.match(line):
            if not self.test:
                timestamp = repr(time.time()) + " -"
            else:
                timestamp =  "000.000 -"
        else:
            timestamp = "  "
        
        output =  "%-20s %s" % (timestamp, line)
        
        return output


    def start(self):
        line = sys.stdin.readline()
        
        while line:
            
            output = self.process_line(line, newCommand)
            
            print output
            if f:
                f.write(output)
                f.write("\n")
            line = sys.stdin.readline()
    
        f.close()

    def __init__(self, options, args, test):
        self.f = None 
        self.new_command = re.compile("^(.*?) (\-\->>|<<\-\-) (.*?)")
        if options:
            self.salt = options.salt        
            if options.out: 
                self.f = open(options.out, 'w')
        else:
            self.salt = 0
        self.test = test

        


if __name__ == '__main__':
    aparser = optparse.OptionParser(description='MongoSniff Trace Anonymizer')
    aparser.add_option('-s', '--salt', dest='salt', type=int,
                         help='Random hash salt')
    aparser.add_option('-o', '--out', dest='out', type=str,
                         help='output file name')
    (options, args) = aparser.parse_args()
    
    s = Sanitizer(options, args, False) # test: false
    s.start()
    
## MAIN        




    
