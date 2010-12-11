#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    virtual uint64_t        GetChunkID() = 0;
    virtual bool            UseBloomFilter();
    
    virtual bool            Get(ReadBuffer& key, ReadBuffer& value);
    
    virtual uint64_t        GetLogSegmentID();
    virtual uint32_t        GetLogCommandID();
    
    virtual uint64_t        GetSize();

    unsigned                numShards;      // this chunk backs this many shards
};

#endif
