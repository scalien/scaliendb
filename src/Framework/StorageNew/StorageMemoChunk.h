#ifndef STORAGEMEMOCHUNK_H
#define STORAGEMEMOCHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageChunk.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileChunk.h"

class StorageFileChunk;

/*
===============================================================================================

 StorageMemoChunk

===============================================================================================
*/

class StorageMemoChunk : public StorageChunk
{
    friend class StorageChunkSerializer;

public:
    typedef InTreeMap<StorageMemoKeyValue> KeyValueTree;
    
    StorageMemoChunk();
    ~StorageMemoChunk();
    
    ChunkState          GetChunkState();
    
    void                SetChunkID(uint64_t chunkID);
    void                SetUseBloomFilter(bool useBloomFilter);
    
    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    
    StorageKeyValue*    Get(ReadBuffer& key);
    bool                Set(ReadBuffer& key, ReadBuffer& value);
    bool                Delete(ReadBuffer& key);
    
    void                RegisterLogCommand(uint64_t logSegmentID, uint32_t logCommandID);
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    
    uint64_t            GetSize();
    
    StorageFileChunk*   RemoveFileChunk();
        
private:
    bool                serialized;
    uint64_t            chunkID;
    uint64_t            logSegmentID;
    uint32_t            logCommandID;
    bool                useBloomFilter;
    uint64_t            size;
    KeyValueTree        keyValues;
    
    StorageFileChunk*   fileChunk; // for serialization
};

#endif
