#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"
#include "StorageChunkFile.h"

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    typedef enum ChunkState {ReadWrite, Serialized, Written} ChunkState;
    typedef InTreeMap<StorageKeyValue> KeyValueTree;
    
    StorageChunk();
    
    void                SetFilename(Buffer& filename);
    void                SetChunkID(uint64_t chunkID);
    void                SetUseBloomFilter(bool useBloomFilter);
    
    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    
    bool                Get(ReadBuffer& firstKey, ReadBuffer& lastKey, ReadBuffer& key, ReadBuffer& value);
    bool                Set(ReadBuffer& key, ReadBuffer& value);
    bool                Delete(ReadBuffer& key);
    
    void                RegisterLogCommand(uint64_t logSegmentID, uint64_t logCommandID);
    uint64_t            GetLogSegmentID();
    uint64_t            GetLogCommandID();
    
    KeyValueTree&       GetKeyValueTree();
    uint64_t            GetSize();
    ChunkState          GetState();
    void                TryFinalize();
    bool                IsFinalized();
    void                WriteFile();
        
private:
    // on disk
    uint64_t            chunkID;
    uint64_t            logSegmentID;
    uint64_t            logCommandID;
    uint64_t            numKeys;
    bool                useBloomFilter;
    
    // in memory:
    ChunkState          state;
    unsigned            numShards;      // this chunk backs this many shards

    // cache:
    KeyValueTree*       keyValues;      // non-NULL if-a-o-if state == InMemory
    StorageChunkFile*   file;           // non-NULL if-a-o-if state != InMemory

    Buffer              filename;
};

#endif
