#!/bin/sh

COMMAND=
COMMAND="valgrind -v --leak-check=full --show-reachable=yes --dsymutil=yes"
BUILD_DIR=build/Debug/
EXECUTABLE=TestProgram

script/maketest.sh $* && $COMMAND $BUILD_DIR/$EXECUTABLE $*
exit $?