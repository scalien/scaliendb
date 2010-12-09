#ifndef STORAGESHARDRECORD_H
#define STORAGESHARDRECORD_H

/*
===============================================================================================

 StorageShardRecord

===============================================================================================
*/

class StorageShardRecord
{
    typedef InTreeNode<StorageShardRecord> TreeNode;

public:
    TreeNode        treeNode;
    
private:
    List<uint64_t>  chunkIDs; // contains all non-deleted chunkIDs
};

#endif
