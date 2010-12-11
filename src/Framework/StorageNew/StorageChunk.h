#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    typedef InTreeNode<StorageChunk> TreeNode;

    virtual uint64_t            GetChunkID() = 0;
    virtual bool                UseBloomFilter() = 0;
    
    virtual StorageKeyValue*    Get(ReadBuffer& key) = 0;
    
    virtual uint64_t            GetLogSegmentID() = 0;
    virtual uint32_t            GetLogCommandID() = 0;
    
    virtual uint64_t            GetSize() = 0;

    unsigned                    numShards;      // this chunk backs this many shards

    TreeNode                    treeNode;
};

#endif
