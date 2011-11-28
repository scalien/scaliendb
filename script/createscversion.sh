#!/bin/sh

COMMIT_ID=`git log -n1 --pretty=oneline | cut -f 1 -d ' '`
OUTPUT=src/SourceControl.h
BRANCH=$(git status | head -1 | sed -n 's/^# On branch //p')

echo "// Auto-generated file" > $OUTPUT
echo "#ifndef SOURCE_CONTROL_H" >> $OUTPUT
echo "#define SOURCE_CONTROL_H" >> $OUTPUT
echo >> $OUTPUT
echo "#define SOURCE_CONTROL_VERSION \"$COMMIT_ID\"" >> $OUTPUT
echo "#define SOURCE_CONTROL_BRANCH \"$BRANCH\"" >> $OUTPUT
echo >> $OUTPUT
echo "#endif" >> $OUTPUT
echo >> $OUTPUT

