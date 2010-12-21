#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

class StorageCursorBunch;

/*
===============================================================================================

 StorageChunk

===============================================================================================
*/

class StorageChunk
{
public:
    typedef enum ChunkState { Tree, Serialized, Unwritten, Written } ChunkState;
    typedef InTreeNode<StorageChunk> TreeNode;

    virtual uint64_t            GetChunkID() = 0;
    virtual bool                UseBloomFilter() = 0;
    
    virtual StorageKeyValue*    Get(ReadBuffer& key) = 0;
    
    virtual uint64_t            GetLogSegmentID() = 0;
    virtual uint32_t            GetLogCommandID() = 0;
    
    virtual uint64_t            GetSize() = 0;
    virtual ChunkState          GetChunkState() = 0;
    
    virtual void                NextBunch(StorageCursorBunch& bunch) = 0;

    TreeNode                    treeNode;
};

#endif
