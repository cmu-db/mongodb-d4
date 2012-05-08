#!/usr/bin/env python

import logging
import argparse
import sys

# globals
INPUT_FILE = "../../data/sample-anon.txt"
OUTPUT_FILE = "out.txt"

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
LOG = logging.getLogger(__name__)

def prep(input, output):
    in_file = file(input, "r")
    out_file = file(output, "w")
    ready = True
    line_out = None
    for line in in_file:
        l2 = line.strip()
        if ready:
            if line_out is not None:
                # write the line
                out_file.write(line_out)
                #pass
            line_out = line 
            if l2.startswith("{"):
                if not l2.endswith("}"): #line not ended properly
                    ready = False
                    print "PROBLEM"
        else:
            # last line was unended... append the next line
            line_out = line_out.rstrip("\n") + line
            
            #print "FIXED LINE"
            #print line_out
            if l2.endswith("}"):
                ready = True
        
                


def main():
    aparser = argparse.ArgumentParser(description='MongoDesigner Parser Prep')
    aparser.add_argument('--in',
                         help='file to read from', default=INPUT_FILE)
    aparser.add_argument('--out',
                         help='file to write to', default=OUTPUT_FILE)
    args = vars(aparser.parse_args())

    settings = "in: ", args['in'], "out: ", args['out']
    LOG.info("Settings: %s", settings)

    prep(args['in'], args['out'])

if __name__ == '__main__':
        main()