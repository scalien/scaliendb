#!/bin/sh

# usage:
#   mkcontrol.sh target-control-file package-name version arch

PACKAGE=$2
VERSION=$3
ARCH=$4

cat > $1 << EOF
Package: $PACKAGE
Version: $VERSION
Section: database
Priority: optional
Depends: libstdc++6
Architecture: $ARCH
Installed-Size: 1024
Maintainer: Scalien Software (info@scalien.com)
Source: $PACKAGE
Description: ScalienDB
  ScalienDB is a consistently replicated, scalable key-value database.

EOF
