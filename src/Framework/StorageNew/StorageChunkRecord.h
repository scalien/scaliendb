#ifndef STORAGECHUNKRECORD_H
#define STORAGECHUNKRECORD_H

/*
===============================================================================================

 StorageChunkRecord

===============================================================================================
*/

class StorageChunkRecord
{
public:
    typedef enum ChunkState {InMemory, WritingToDisk, OnDisk} ChunkState;
    
    // on disk
    uint64_t    chunkID;
    uint64_t    shardID;        // this is the shardID this chunk originally belonged to
    uint64_t    logSegmentID;
    uint64_t    logCommandID;
    uint64_t    numKeys;
    bool        useBloomFilter;
    
    // in memory:
    bool        readOnly;       // whether this chunk has been closed for writing
    unsigned    numShards;      // this chunk backs this many shards
    ChunkState  state;
    bool        merged;
    uint64_t    parent1;        // if merged
    uint64_t    parend2;        // if merged
    
    // cache:
    FD          fd;             // INVALID_FD if the file is not open
    
};

#endif
