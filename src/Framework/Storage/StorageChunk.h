#ifndef STORAGECHUNK_H
#define STORAGECHUNK_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

class StorageCursorBunch;
class StorageShard;

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

    virtual ~StorageChunk() {}

    virtual uint64_t            GetChunkID() = 0;
    virtual bool                UseBloomFilter() = 0;
    
    virtual StorageKeyValue*    Get(ReadBuffer& key) = 0;
    
    virtual uint64_t            GetMinLogSegmentID() = 0;
    virtual uint64_t            GetMaxLogSegmentID() = 0;
    virtual uint32_t            GetMaxLogCommandID() = 0;
    
    virtual ReadBuffer          GetFirstKey() = 0;
    virtual ReadBuffer          GetLastKey() = 0;
    
    virtual uint64_t            GetSize() = 0;
    virtual ReadBuffer          GetMidpoint() = 0;
    virtual ChunkState          GetChunkState() = 0;
    
    virtual void                NextBunch(StorageCursorBunch& bunch, StorageShard* shard) = 0;

    TreeNode                    treeNode;
};

#endif
