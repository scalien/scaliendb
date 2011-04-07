#ifndef CONFIGSHARD_H
#define CONFIGSHARD_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/Buffers/Buffer.h"

#define CONFIG_SHARD_STATE_NORMAL              0
#define CONFIG_SHARD_STATE_SPLIT_CREATING      1
#define CONFIG_SHARD_STATE_TRUNC_CREATING      2

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

//    bool            isSplitCreating;
    unsigned        state;
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
