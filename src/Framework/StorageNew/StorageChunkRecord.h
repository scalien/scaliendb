#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    typedef enum ChunkState {InMemory, WritingToDisk, OnDisk} ChunkState;
    typedef InTreeMap<StorageKeyValue> KeyValueTree;
    
    // shardID is not stored here, since a shard can belong to more than one shard
    // in case of shard splitting

    // on disk
    uint64_t            chunkID;
    uint64_t            logSegmentID;
    uint64_t            logCommandID;
    uint64_t            numKeys;
    bool                useBloomFilter;
    uint64_t            numKeys;
    
    // in memory:
    ChunkState          state;
    unsigned            numShards;      // this chunk backs this many shards

    bool                IsMerged();
    uint64_t            parent1;        // if merged
    uint64_t            parent2;        // if merged
    
    // cache:
    KeyValueTree*       tree;   // non-NULL if-a-o-if state == InMemory
    StorageChunkFile*   file;   // non-NULL if-a-o-if state != InMemory
};

class StorageChunkFile
{
    typedef InList<StorageChunkDataPage> DataPages;

public:
    StorageChunkHeaderPage  headerPage;
    StorageChunkIndexPage   indexPage;
    StorageChunkBloomPage   bloomPage;

    DataPages               dataPages;
};

#endif
