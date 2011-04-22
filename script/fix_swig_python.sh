#!/bin/sh
#
# This script removes deprecated 2.x features from the SWIG generated
# Python client library.
#

DIR=$1

mv $DIR/scaliendb_client.py $DIR/scaliendb_client_swig.py
cat $DIR/scaliendb_client_swig.py | sed 's/^import new$//' | sed 's/^new_instancemethod = new.instancemethod$//' | sed 's/^    def __eq__(\*args): return _scaliendb_client.PySwigIterator___eq__(\*args)$//' > $DIR/scaliendb_client.py
rm $DIR/scaliendb_client_swig.py
