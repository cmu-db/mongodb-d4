#!/usr/bin/env python
import sys
sys.path.append("../sanitizer")
import anonymize # just for hash_string()


expected_plain = "\"drivers\""
expected_hash = "c9f685688b90e80b8055ef9f1d72b7ce/9"
salt = 0
while True:
    hash = anonymize.hash_string(expected_plain, salt)
    print "salt ", salt, ": ", hash
    if hash == expected_hash:
        print "FOUND SALT: ", salt
    salt += 1
print "Done."