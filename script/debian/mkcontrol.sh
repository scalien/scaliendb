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
Depends: libstdc++6, libdb4.4++|libdb4.5++|libdb4.6++
Architecture: $ARCH
Installed-Size: 1024
Maintainer: Scalien Software (info@scalien.com)
Source: $PACKAGE
Description: Scalien Keyspace DB
  Keyspace is a consistently replicated key-value database.

EOF
