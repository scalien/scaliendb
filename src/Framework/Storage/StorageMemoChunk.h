#ifndef STORAGEMEMOCHUNK_H
#define STORAGEMEMOCHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/InQueue.h"
#include "StorageChunk.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileChunk.h"

#define STORAGE_MEMO_BUNCH_GRAN     1*MB
#define STORAGE_BLOCK_NUM_KEY_VALUE ((128*KiB)/sizeof(StorageMemoKeyValue))

class StorageFileChunk;

struct StorageMemoKeyValueBlock
{
    StorageMemoKeyValue         keyValues[STORAGE_BLOCK_NUM_KEY_VALUE];
    StorageMemoKeyValueBlock*   next;
    unsigned                    first;
    unsigned                    last;
};

/*
===============================================================================================

 StorageMemoChunk

===============================================================================================
*/

class StorageMemoChunk : public StorageChunk
{
    friend class StorageChunkSerializer;
    friend class StorageEnvironment;
    friend class StorageMemoChunkLister;

public:
    typedef InTreeMap<StorageMemoKeyValue> KeyValueTree;
    typedef InQueue<StorageMemoKeyValueBlock> KeyValueBlockQueue;
    
    StorageMemoChunk(uint64_t chunkID, bool useBloomFilter);
    ~StorageMemoChunk();
    
    ChunkState              GetChunkState();
    
    void                    NextBunch(StorageBulkCursor& cursor, StorageShard* shard);
        
    uint64_t                GetChunkID();
    bool                    UseBloomFilter();
    
    StorageKeyValue*        Get(ReadBuffer& key);
    bool                    Set(ReadBuffer key, ReadBuffer value);
    bool                    Delete(ReadBuffer key);

    void                    AsyncGet(StorageAsyncGet* asyncGet);
    
    void                    RegisterLogCommand(uint64_t logSegmentID, uint32_t logCommandID);
    uint64_t                GetMinLogSegmentID();
    uint64_t                GetMaxLogSegmentID();
    uint32_t                GetMaxLogCommandID();

    ReadBuffer              GetFirstKey();
    ReadBuffer              GetLastKey();
    
    uint64_t                GetSize();
    ReadBuffer              GetMidpoint();
    
    bool                    IsEmpty();
    
    StorageFileChunk*       RemoveFileChunk();

    void                    RemoveFirst(); // for logstorage

    StorageMemoKeyValue*    NewStorageMemoKeyValue();

private:
    bool                    serialized;
    uint64_t                chunkID;
    uint64_t                minLogSegmentID;
    uint64_t                maxLogSegmentID;
    uint32_t                maxLogCommandID;
    bool                    useBloomFilter;
    uint64_t                size;
    KeyValueTree            keyValues;
    
    StorageFileChunk*       fileChunk; // for serialization
    KeyValueBlockQueue      keyValueBlocks;
};

#endif
