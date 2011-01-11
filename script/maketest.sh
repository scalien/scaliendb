#!/bin/sh

make testobjs
python test/stf.py -c $*
