#!/bin/sh

VERSION=$(script/version.sh 3 src/Version.h)
JAVA_FILES="java/libscaliendb_client.so java/scaliendb.jar"
PYTHON_FILES="python/_scaliendb_client.so python/scaliendb.py python/scaliendb_client.py"
SCALIENDB_FILES="scaliendb safe_scaliendb"

DIR=$(pwd)
cd bin
zip scaliendb-$VERSION.zip $JAVA_FILES $PYTHON_FILES $SCALIENDB_FILES
cd $DIR
