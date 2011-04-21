#!/bin/sh

if [ "$SCALIENDB_HOME" = "" ]; then
	export SCALIENDB_HOME=$(pwd)
fi

export SCALIENDB_PYTHONSTARTUP=$SCALIENDB_HOME/bin/shell.py

export SCALIENDB_SHARDSERVERS="blade6 blade7 blade8 blade9"