#ifndef CONFIGSHARD_H
#define CONFIGSHARD_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/Buffers/Buffer.h"

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
    uint64_t        tableID;
    uint64_t        shardID;

    // ========================================================================================
    //
    // shard splitting

    bool            isSplitCreating;
    uint64_t        parentShardID;
    uint64_t        shardSize;
    Buffer          splitKey;
    bool            isSplitable;

    // ========================================================================================
    
    
    Buffer          firstKey;
    Buffer          lastKey;
    
    ConfigShard*    prev;
    ConfigShard*    next;
};

#endif
