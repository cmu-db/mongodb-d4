#!/bin/sh -x

nosetests --verbose --nocapture $(find . -name "unittest*.py" -type f)