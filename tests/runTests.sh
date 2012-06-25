#!/bin/sh -x

nosetests --verbose --nocapture $(find . -name "*.py" -type f)