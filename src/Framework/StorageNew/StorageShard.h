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

    uint64_t            GetTableID();
    uint64_t            GetShardID();

    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    bool                RangeContains(ReadBuffer& key);

    StorageMemoChunk*   GetMemoChunk();
    void                SetNewMemoChunk(StorageMemoChunk* memoChunk);

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

    ChunkList           chunks; // contains all active chunks, in order, first is the write chunk
};

#endif
