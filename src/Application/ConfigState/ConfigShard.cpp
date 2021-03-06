#include "ConfigShard.h"

ConfigShard::ConfigShard()
{
    prev = next = this;
    state = CONFIG_SHARD_STATE_NORMAL;
//    isSplitCreating = false;
    parentShardID = 0;
    shardSize = 0;
    isSplitable = false;
}

ConfigShard::ConfigShard(const ConfigShard& other)
{
    *this = other;
}

ConfigShard& ConfigShard::operator=(const ConfigShard& other)
{
    quorumID = other.quorumID;
    tableID = other.tableID;
    shardID = other.shardID;
    
    firstKey = other.firstKey;
    lastKey = other.lastKey;
    
//    isSplitCreating = other.isSplitCreating;
    state = other.state;
    parentShardID = other.parentShardID;

    shardSize = other.shardSize;
    splitKey = other.splitKey;
    isSplitable = other.isSplitable;
    
    prev = next = this;
    
    return *this;
}
