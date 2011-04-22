#!/bin/sh

DIR=$1

mv $DIR/scaliendb_client.php $DIR/scaliendb_client_swig.php
cat $DIR/scaliendb_client_swig.php | sed 's/(function_exists($func) /(function_exists($func)) /' > $DIR/scaliendb_client.php
rm $DIR/scaliendb_client_swig.php
