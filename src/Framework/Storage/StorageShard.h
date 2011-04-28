#ifndef STORAGESHARD_H
#define STORAGESHARD_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/SortedList.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"

class StorageRecovery;
class StorageBulkCursor;

/*
===============================================================================================

 StorageShard

===============================================================================================
*/

class StorageShard
{
    friend class StorageRecovery;
    friend class StorageBulkCursor;
    
public:
    typedef SortedList<StorageChunk*> ChunkList;

    StorageShard();
    ~StorageShard();

    void                SetContextID(uint16_t contextID);
    void                SetTableID(uint64_t tableID);
    void                SetShardID(uint64_t shardID);
    void                SetLogSegmentID(uint64_t logSegmentID);
    void                SetLogCommandID(uint64_t logCommandID);
    void                SetFirstKey(ReadBuffer firstKey);
    void                SetLastKey(ReadBuffer lastKey);
    void                SetUseBloomFilter(bool useBloomFilter);
    void                SetLogStorage(bool isLogStorage);

    uint16_t            GetContextID();
    uint64_t            GetTableID();
    uint64_t            GetShardID();
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    bool                UseBloomFilter();
    bool                IsLogStorage();
    
    bool                RangeContains(ReadBuffer key);

    void                PushMemoChunk(StorageMemoChunk* memoChunk);
    void                PushChunk(StorageChunk* chunk);
    StorageMemoChunk*   GetMemoChunk();
    ChunkList&          GetChunks();
    void                OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);
    void                GetMergeInputChunks(List<StorageFileChunk*>& inputChunks);


    StorageShard*       prev;
    StorageShard*       next;

    StorageMemoChunk*   memoChunk;

private:
    uint16_t            contextID;
    uint64_t            shardID;
    uint64_t            tableID;
    uint64_t            logSegmentID;   // the first command
    uint32_t            logCommandID;   // which may affect this shard
    Buffer              firstKey;
    Buffer              lastKey;
    bool                useBloomFilter;
    bool                isLogStorage;

    ChunkList           chunks;
    
    uint64_t            recoveryLogSegmentID; // only used
    uint32_t            recoveryLogCommandID; // during log recovery
};

inline bool LessThan(StorageChunk* a, StorageChunk* b)
{
    if (a->GetMaxLogSegmentID() < b->GetMaxLogSegmentID())
        return true;
    else if (a->GetMaxLogSegmentID() == b->GetMaxLogSegmentID())
        return (a->GetMaxLogCommandID() < b->GetMaxLogCommandID());
    else
        return false;
}

#endif
