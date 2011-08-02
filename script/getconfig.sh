#!/bin/sh

# check command line arguments
if [ "$1" = "" ]; then
	echo
	echo "Usage:"
	echo
	echo "    getconfig.sh <config-url>"
	echo
	echo
	exit 1
fi

# check if dependencies are installed
DEPENDENCIES="awk sed curl mktemp /sbin/ifconfig uname"
for dep in $DEPENDENCIES; do
	DEPPATH=$(which $dep)
	if [ $? != 0 ]; then
		echo "Cannot find $dep! Please install it before running getconfig!"
		exit 1
	fi
done

CONFIG_URL=$1

# create temp file and delete it on exit
TEMP_CONFIG_FILE=$(mktemp /tmp/scaliendb-controller-config.XXXXXX)
trap "rm $TEMP_CONFIG_FILE" EXIT

# find out endpoints
UNAME=$(uname)
if [ "$UNAME" = "Linux" ]; then INET_PREFIX="inet addr:"; fi
if [ "$UNAME" = "Darwin" ]; then INET_PREFIX="inet "; fi
INET_ADDRESSES=$(/sbin/ifconfig  | grep "$INET_PREFIX" | sed 's/'"$INET_PREFIX"'\(\([[:digit:]]\{1,3\}\.\)\{3\}[[:digit:]]\{1,3\}\).*/\1/')
DEFAULT_PORT=10000
ENDPOINTS=
for addr in $INET_ADDRESSES; do
	ENDPOINTS="$ENDPOINTS $addr:$DEFAULT_PORT"
done

# download config file
curl -s $CONFIG_URL > $TEMP_CONFIG_FILE
if [ $? != 0 ]; then
	echo "Cannot download config file!"
	exit 1
fi

# find out controllers from config file
CONTROLLERS=$(sed -n "s/^[^#[:space:]]*controllers[[:space:]]*=[[:space:]]*\(.*\)$/\1/p" $TEMP_CONFIG_FILE | tail -n 1)
if [ "$CONTROLLERS" = "" ]; then
	echo "Cannot find controllers in config file!"
	exit 1
fi

# find out nodeID by endpoints
NODEID_ENDPOINT=$(echo | awk "BEGIN { controllers=\"$CONTROLLERS\"; endpoints=\"$ENDPOINTS\" }"\
'END { 
	gsub(/[ \t]+/, "", controllers);
	nc = split(controllers, c, ",");
	ne = split(endpoints, e, " ");
	for (i = 1; i <= nc; i++) { 
		for (j = 1; j < ne; j++) {
			gsub(/[ \t]+/, "", e[j]);
			if (c[i] == e[j]) { 
				print i-1, e[j]; 
				exit;
			}
		}
	}
}')

if [ "$NODEID_ENDPOINT" = "" ]; then
	echo "Cannot find the given endpoint in config file!"
	exit 1
fi

NODEID=$(echo $NODEID_ENDPOINT | sed 's/\([[:digit:]]*\) \(.*\)/\1/')
if [ "$NODEID" = "" ]; then
	echo "Cannot find the nodeID in config file!"
	exit 1
fi

ENDPOINT=$(echo $NODEID_ENDPOINT | sed 's/\([[:digit:]]*\) \(.*\)/\2/')
if [ "$ENDPOINT" = "" ]; then
	echo "Cannot find the endpoint in config file!"
	exit 1
fi

# replace nodeID and endpoint in config file
cat $TEMP_CONFIG_FILE \
| sed "s/^[^#[:space:]]*nodeID[[:space:]]*=[[:space:]]*[[:digit:]]*.*/nodeID = $NODEID/" \
| sed "s/^[^#[:space:]]*endpoint[[:space:]]*=[[:space:]].*$/endpoint = $ENDPOINT/"
