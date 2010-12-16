#ifndef STORAGESHARD_H
#define STORAGESHARD_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/SortedList.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"

/*
===============================================================================================

 StorageShard

===============================================================================================
*/

class StorageShard
{
public:
    typedef SortedList<StorageChunk*> ChunkList;

    StorageShard();

    void                SetContextID(uint16_t contextID);
    void                SetTableID(uint64_t tableID);
    void                SetShardID(uint64_t shardID);
    void                SetLogSegmentID(uint64_t logSegmentID);
    void                SetLogCommandID(uint64_t logCommandID);
    void                SetFirstKey(ReadBuffer& firstKey);
    void                SetLastKey(ReadBuffer& lastKey);
    void                SetUseBloomFilter(bool useBloomFilter);

    uint16_t            GetContextID();
    uint64_t            GetTableID();
    uint64_t            GetShardID();
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    bool                UseBloomFilter();
    
    bool                RangeContains(ReadBuffer& key);

    void                PushMemoChunk(StorageMemoChunk* memoChunk);
    StorageMemoChunk*   GetMemoChunk();
    ChunkList&          GetChunks();
    void                OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);

    StorageMemoChunk*   memoChunk;

    StorageShard*       prev;
    StorageShard*       next;

private:
    uint16_t            contextID;
    uint64_t            shardID;
    uint64_t            tableID;
    uint64_t            logSegmentID;   // the first command
    uint32_t            logCommandID;   // which may affect this shard
    Buffer              firstKey;
    Buffer              lastKey;
    bool                useBloomFilter;

    ChunkList           chunks; // contains all active chunks, in order, first is the write chunk
};

#endif
