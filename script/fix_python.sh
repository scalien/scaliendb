#!/bin/sh
#
# This script changes exception syntax for deprecated 2.x syntax.
#

PYTHON_MAJOR_VERSION=$(python -V 2>&1 | sed 's/Python \(.\)\..*/\1/')

if [ "$PYTHON_MAJOR_VERSION" = "2" ]; then
        echo 2
        cat $1 | sed 's/except \(.*\) as \(.*\):/except \1, \2:/'
else
        cat $1
fi
