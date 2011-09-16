#!/bin/sh
#
# Find and count all conditiona in all the .cpp files in the given directory, 
# except SWIG generated scaliendb_client files.
#

find $1 -name '*.cpp' -a -not -name 'scaliendb_client*' -exec grep 'if (' {} \; | grep -v '^//' | wc -l
