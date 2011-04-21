#!/bin/sh

. $(dirname $0)/scaliendb-env.sh

CMD=$*

for shard_server in $SCALIENDB_SHARDSERVERS; do
        remote_cmd="cd scaliendb; $CMD"
        echo "$shard_server:$$ $remote_cmd"
        remote_cmd_redirect="$remote_cmd 2>&1 1>/dev/null"
        ssh $shard_server $remote_cmd_redirect 2>&1 1>/dev/null | sed s/^/$shard_server: / &
done

wait
