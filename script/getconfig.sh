#!/bin/sh

if [ "$1" = "" -o "$2" = "" ]; then
	echo
	echo "Usage:"
	echo
	echo "    getconfig.sh <endpoint> <config-url>"
	echo
	echo
	exit 1
fi

DEPENDENCIES="awk sed curl mktemp"
for dep in $DEPENDENCIES; do
	DEPPATH=$(which $dep)
	if [ $? != 0 ]; then
		echo "Cannot find $dep! Please install it before running getconfig!"
		exit 1
	fi
done

ENDPOINT=$1
CONFIG_URL=$2

TEMP_CONFIG_FILE=$(mktemp /tmp/scaliendb-controller-config.XXXXXX)
trap "rm $TEMP_CONFIG_FILE" EXIT

curl -s $CONFIG_URL > $TEMP_CONFIG_FILE
if [ $? != 0 ]; then
	echo "Cannot download config file!"
	exit 1
fi

#CONTROLLERS=127.0.0.1:7080,127.0.0.1:7081,127.0.0.1:7082
CONTROLLERS=$(sed -n "s/^[^#[:space:]]*controllers[[:space:]]*=[[:space:]]*\(.*\)$/\1/p" $TEMP_CONFIG_FILE | tail -n 1)
if [ "$CONTROLLERS" = "" ]; then
	echo "Cannot find controllers in config file!"
	exit 1
fi

NODEID=$(echo | awk "BEGIN { controllers=\"$CONTROLLERS\"; endpoint=\"$ENDPOINT\" }"\
'END { n = split(controllers, c, ","); for (i = 1; i <= n; i++) if (c[i] == endpoint) { print i-1; break } }')
if [ "$NODEID" = "" ]; then
	echo "Cannot find the given endpoint in config file!"
	exit 1
fi

cat $TEMP_CONFIG_FILE \
| sed "s/^[^#[:space:]]*nodeID[[:space:]]*=[[:space:]]*[[:digit:]]*/nodeID = $NODEID/" \
| sed "s/^[^#[:space:]]*controllers[[:space:]]*=.*/controllers = $CONTROLLERS/" \

