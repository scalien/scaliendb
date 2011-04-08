#!/bin/sh

SHARD_SERVERS='blade6 blade7 blade8 blade9'
CMD=$*

for shard_server in $SHARD_SERVERS; do
        remote_cmd="cd scaliendb; $CMD"
        ssh $shard_server $remote_cmd > /dev/null &
        sleep 1
done
