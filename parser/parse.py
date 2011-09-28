#!/usr/bin/env python
import sys
import fileinput
import hashlib
import time
import re


INPUT_FILE = "sample.txt"


TIME_MASK = "[0-9]+\.[0-9]+.*"
ARROW_MASK = "(-->>|<<--)"
IP_MASK = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{5,5}"
COLLECTION_MASK = "\w+\.\$?\w+"
SIZE_MASK = "\d+ bytes"
MAGIC_ID_MASK = "id:\w+ \d+"
REPLY_ID_MASK = "\d+"


HEADER_MASK = "(?P<timestamp>" + TIME_MASK + ") *- *" + \
"(?P<IP1>" + IP_MASK + ") *" + \
"(?P<arrow>" + ARROW_MASK + ") *" + \
"(?P<IP2>" + IP_MASK + ") *" + \
"(?P<collection>" + COLLECTION_MASK + ")? *" + \
"(?P<size>" + SIZE_MASK + ") *" + \
"(?P<magic_id>" + MAGIC_ID_MASK + ") *" + \
"-? *(?P<reply_id>" + REPLY_ID_MASK + ")?"


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
            m = re.search(HEADER_MASK, line)
            if (m):
                print(m.groupdict())
            else:
                print(line)
                exit()
        else:
            print("NO.")
        
        
    return
        

if __name__ == '__main__':
	main()



    
