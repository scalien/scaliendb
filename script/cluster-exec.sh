#!/bin/sh

. $(dirname $0)/scaliendb-env.sh

CMD=$*

for shard_server in $SCALIENDB_SHARDSERVERS; do
        remote_cmd="cd scaliendb; $CMD"
        echo $remote_cmd
        remote_cmd_redirect="$remote_cmd 2>&1 1>/dev/null | sed -e s/^/$shard_server/"
        ssh $shard_server $remote_cmd_redirect > /dev/null &
done

wait
