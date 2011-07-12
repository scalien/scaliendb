#ifndef STORAGEMEMOCHUNK_H
#define STORAGEMEMOCHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/InQueue.h"
#include "System/Containers/InList.h"
#include "StorageChunk.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileChunk.h"

#define STORAGE_MEMO_BUNCH_GRAN             1*MB
#define STORAGE_MEMO_ALLOCATOR_DEFAULT_SIZE 16*KiB
#define STORAGE_MEMO_ALLOCATOR_MIN_SIZE     128
#define STORAGE_BLOCK_NUM_KEY_VALUE     \
    ((128*KiB-sizeof(void*)-2*sizeof(unsigned))/sizeof(StorageMemoKeyValue))

class StorageFileChunk;

struct StorageMemoKeyValueBlock
{
    StorageMemoKeyValue         keyValues[STORAGE_BLOCK_NUM_KEY_VALUE];
    StorageMemoKeyValueBlock*   next;
    unsigned                    first;
    unsigned                    last;
};

struct StorageMemoKeyValueAllocator
{
    uint32_t                        GetFreeSize();                        

    char*                           buffer;
    uint32_t                        pos;
    uint32_t                        size;
    uint16_t                        num;
    StorageMemoKeyValueAllocator*   next;
    StorageMemoKeyValueAllocator*   prev;
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
    typedef InList<StorageMemoKeyValueAllocator> AllocatorList;
    
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
    char*                   Alloc(StorageMemoKeyValue* keyValue, size_t size);
    void                    Free(StorageMemoKeyValue* keyValue, char* buffer);

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
    AllocatorList           allocators;
};

#endif
