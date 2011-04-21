#!/bin/sh

. bin/scaliendb-env.sh

echo $SCALIENDB_PYTHONSTARTUP
export PYTHONSTARTUP=$SCALIENDB_PYTHONSTARTUP

make pythonlib &> /dev/null
cd bin/python
python 
