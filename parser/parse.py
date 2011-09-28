#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time
import re


INPUT_FILE = "sample.txt"
#IP_REGEX = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{5,5}.*"
TIME_MASK = "[0-9]+\.[0-9]+.*"

def main():
    #for line in sys.stdin:
        #if is_important(line):
        #    print timestamp(sanitize(line))
     #   print line,
    file = open(INPUT_FILE, 'r')
    line = file.readline()
    while line:
        line = file.readline()
        if re.match(TIME_MASK, line):
            print("MATCH")
        else:
            print("NO.")
        print (line)
        
        
    return
        

if __name__ == '__main__':
	main()



    
