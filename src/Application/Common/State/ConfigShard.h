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
    ConfigShard()   { prev = next = this; }

    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    
    Buffer          firstKey;
    Buffer          lastKey;
    
    ConfigShard*    prev;
    ConfigShard*    next;
};

#endif
