#include "ConfigShard.h"

ConfigShard::ConfigShard()
{
    prev = next = this;
}

ConfigShard::ConfigShard(const ConfigShard& other)
{
    *this = other;
}

ConfigShard& ConfigShard::operator=(const ConfigShard& other)
{
    quorumID = other.quorumID;
    databaseID = other.databaseID;
    tableID = other.tableID;
    shardID = other.shardID;
    
    firstKey = other.firstKey;
    lastKey = other.lastKey;
    
    prev = next = this;
    
    return *this;
}
