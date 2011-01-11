#!/bin/sh

COMMAND=
BUILD_DIR=build/Debug/
EXECUTABLE=TestProgram

script/maketest.sh $*
$COMMAND $BUILD_DIR/$EXECUTABLE $*
