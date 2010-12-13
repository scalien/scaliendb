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
    typedef SortedList<StorageChunk*> ChunkList;

public:

    StorageShard();

    void                SetTableID(uint64_t tableID);
    void                SetShardID(uint64_t shardID);
    void                SetFirstKey(ReadBuffer& firstKey);
    void                SetLastKey(ReadBuffer& lastKey);
    void                SetUseBloomFilter(bool useBloomFilter);

    uint64_t            GetTableID();
    uint64_t            GetShardID();
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    bool                UseBloomFilter();
    
    bool                RangeContains(ReadBuffer& key);

    void                PushMemoChunk(StorageMemoChunk* memoChunk);
    StorageMemoChunk*   GetMemoChunk();
    void                OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);

    StorageMemoChunk*   memoChunk;
    StorageChunk**      First();
    StorageChunk**      Last();
    StorageChunk**      Next(StorageChunk** curr);
    StorageChunk**      Prev(StorageChunk** curr);

    StorageShard*       prev;
    StorageShard*       next;

private:
    uint64_t            shardID;
    uint64_t            tableID;
    Buffer              firstKey;
    Buffer              lastKey;
    bool                useBloomFilter;

    ChunkList           chunks; // contains all active chunks, in order, first is the write chunk
};

#endif
