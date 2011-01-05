#ifndef STORAGEMEMOCHUNK_H
#define STORAGEMEMOCHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageChunk.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileChunk.h"

#define STORAGE_MEMO_BUNCH_GRAN     1*MB

class StorageFileChunk;

/*
===============================================================================================

 StorageMemoChunk

===============================================================================================
*/

class StorageMemoChunk : public StorageChunk
{
    friend class StorageChunkSerializer;
    friend class StorageEnvironment;

public:
    typedef InTreeMap<StorageMemoKeyValue> KeyValueTree;
    
    StorageMemoChunk();
    ~StorageMemoChunk();
    
    ChunkState          GetChunkState();
    
    void                NextBunch(StorageCursorBunch& bunch, StorageShard* shard);
    
    void                SetChunkID(uint64_t chunkID);
    void                SetUseBloomFilter(bool useBloomFilter);
    
    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    
    StorageKeyValue*    Get(ReadBuffer& key);
    bool                Set(ReadBuffer key, ReadBuffer value);
    bool                Delete(ReadBuffer key);
    
    void                RegisterLogCommand(uint64_t logSegmentID, uint32_t logCommandID);
    uint64_t            GetMinLogSegmentID();
    uint64_t            GetMaxLogSegmentID();
    uint32_t            GetMaxLogCommandID();
    
    uint64_t            GetSize();
    
    StorageFileChunk*   RemoveFileChunk();
        
private:
    bool                serialized;
    uint64_t            chunkID;
    uint64_t            minLogSegmentID;
    uint64_t            maxLogSegmentID;
    uint32_t            maxLogCommandID;
    bool                useBloomFilter;
    uint64_t            size;
    KeyValueTree        keyValues;
    
    StorageFileChunk*   fileChunk; // for serialization
};

#endif
