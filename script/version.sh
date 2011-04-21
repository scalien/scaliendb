#!/bin/sh

VERSION=
if [ $1 -gt 0 ]; then
	VERSION=`sed -n 's/.*VERSION_MAJOR[[:space:]]*\"\([[:digit:]]*\)\"/\1/p' $2`
fi
if [ $1 -gt 1 ]; then
	VERSION=$VERSION.`sed -n 's/.*VERSION_MINOR[[:space:]]*\"\([[:digit:]]*\)\"/\1/p' $2`
fi
if [ $1 -gt 2 ]; then
	VERSION=$VERSION.`sed -n 's/.*VERSION_RELEASE[[:space:]]*\"\([[:digit:]]*\)\"/\1/p' $2`
fi

echo $VERSION

