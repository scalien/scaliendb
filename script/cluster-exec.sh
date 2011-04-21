#!/bin/sh

. $(dirname $0)/scaliendb-env.sh

CMD=$*

for shard_server in $SCALIENDB_SHARDSERVERS; do
        remote_cmd="cd scaliendb; $CMD"
        ssh $shard_server $remote_cmd > /dev/null &
done

wait
