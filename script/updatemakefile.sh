#!/bin/sh

python script/createmakedirs.py > Makefile.dirs
python script/xcode2make.py > Makefile.objects
