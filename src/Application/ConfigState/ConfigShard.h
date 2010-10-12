#ifndef CONFIGSHARD_H
#define CONFIGSHARD_H

#include "System/Common.h"
#include "System/Containers/List.h"

/*
===============================================================================================

 ConfigShard

===============================================================================================
*/

class ConfigShard
{
public:
    ConfigShard();
    ConfigShard(const ConfigShard& other);
    
    ConfigShard&    operator=(const ConfigShard& other);

    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    
    Buffer          firstKey;
    Buffer          lastKey;
    
    ConfigShard*    prev;
    ConfigShard*    next;
};

inline ConfigShard::ConfigShard()
{
    prev = next = this;
}

inline ConfigShard::ConfigShard(const ConfigShard& other)
{
    *this = other;
}

inline ConfigShard& ConfigShard::operator=(const ConfigShard& other)
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

#endif
