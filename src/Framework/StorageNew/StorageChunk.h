#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"
#include "StorageChunkFile.h"

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    typedef enum ChunkState {ReadWrite, Serialized, Writing, Written} ChunkState;
    typedef InTreeMap<StorageKeyValue> KeyValueTree;
    
    StorageChunk();
    
    void                SetChunkID(uint64_t chunkID);
    void                SetUseBloomFilter(bool useBloomFilter);
    
    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    
    bool                Get(ReadBuffer& firstKey, ReadBuffer& lastKey, ReadBuffer& key, ReadBuffer& value);
    bool                Set(ReadBuffer& key, ReadBuffer& value);
    bool                Delete(ReadBuffer& key);
    
    void                RegisterLogCommand(uint64_t logSegmentID, uint64_t logCommandID);
    uint64_t            GetLogSegmentID();
    uint64_t            GetLogCommandID();
    
    uint64_t            GetNumKeys();
    uint64_t            GetSize();
    ChunkState          GetState();
    
    void                TryFinalize();
    bool                IsFinalized();
        
private:
    // on disk
    uint64_t            chunkID;
    uint64_t            logSegmentID;
    uint64_t            logCommandID;
    uint64_t            numKeys;
    bool                useBloomFilter;
    
    // in memory:
    ChunkState          state;
    unsigned            numShards;      // this chunk backs this many shards

    uint64_t            parent1;        // if merged
    uint64_t            parent2;        // if merged
    
    // cache:
    KeyValueTree*       tree;               // non-NULL if-a-o-if state == InMemory
    StorageChunkFile*   file;   // non-NULL if-a-o-if state != InMemory
};

    // shardID is not stored here, since a shard can belong to more than one shard
    // in case of shard splitting


#endif
