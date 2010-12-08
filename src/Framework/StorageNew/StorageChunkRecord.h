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

    bool                IsMerged();
    
    // cache:
    KeyValueTree*       tree;               // non-NULL if-a-o-if state == InMemory

private:
    bool                useBloomFilter;     // on disk
    uint64_t            chunkID;            // on disk
    uint64_t            logSegmentID;       // on disk
    uint64_t            logCommandID;       // on disk
    uint64_t            numKeys;            // on disk

    ChunkState          state;              // in memory
    unsigned            numShards;          // in memory

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
