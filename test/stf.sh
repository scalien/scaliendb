#!/bin/sh

make testlib
python test/stf.py $*
