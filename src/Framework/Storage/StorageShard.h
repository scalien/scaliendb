#ifndef STORAGESHARD_H
#define STORAGESHARD_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/SortedList.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"

class StorageRecovery;
class StorageBulkCursor;

#define STORAGE_SHARD_TYPE_STANDARD         'F'
#define STORAGE_SHARD_TYPE_LOG              'T'
#define STORAGE_SHARD_TYPE_DUMP             'D'

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
    typedef bool (StorageShard::*IsMergeCandidateFunc)();
    
    StorageShard();
    ~StorageShard();

    void                SetTrackID(uint64_t trackID);
    void                SetContextID(uint16_t contextID);
    void                SetTableID(uint64_t tableID);
    void                SetShardID(uint64_t shardID);
    void                SetLogSegmentID(uint64_t logSegmentID);
    void                SetLogCommandID(uint64_t logCommandID);
    void                SetFirstKey(ReadBuffer firstKey);
    void                SetLastKey(ReadBuffer lastKey);
    void                SetUseBloomFilter(bool useBloomFilter);
    void                SetStorageType(char type);

    uint64_t            GetTrackID();
    uint16_t            GetContextID();
    uint64_t            GetTableID();
    uint64_t            GetShardID();
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    ReadBuffer          GetMidpoint();
    uint64_t            GetSize();
    bool                UseBloomFilter();
    char                GetStorageType();
    bool                IsSplitable();
    bool                IsBackingLogSegment(uint64_t trackID, uint64_t logSegmentID);
    
    bool                RangeContains(ReadBuffer key);

    void                PushMemoChunk(StorageMemoChunk* memoChunk);
    void                PushChunk(StorageChunk* chunk);
    StorageMemoChunk*   GetMemoChunk();
    ChunkList&          GetChunks();
    void                OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);
    bool                IsMergeableType();
    bool                IsSplitMergeCandidate();
    bool                IsFragmentedMergeCandidate();
    void                GetMergeInputChunks(List<StorageFileChunk*>& inputChunks);


    StorageShard*       prev;
    StorageShard*       next;

    StorageMemoChunk*   memoChunk;

private:
    uint64_t            trackID;
    uint16_t            contextID;
    uint64_t            shardID;
    uint64_t            tableID;
    uint64_t            logSegmentID;   // the first command
    uint32_t            logCommandID;   // which may affect this shard
    Buffer              firstKey;
    Buffer              lastKey;
    bool                useBloomFilter;
    char                storageType;

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
