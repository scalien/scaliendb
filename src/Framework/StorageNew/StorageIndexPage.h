#ifndef STORAGEINDEXPAGE_H
#define STORAGEINDEXPAGE_H

#include "StoragePage.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"

class StorageIndexRecord
{
    typedef InTreeNode<StorageIndexRecord> TreeNode;
public:
    ReadBuffer      key;
    uint32_t        index;
    uint32_t        offset;
    
    TreeNode        treeNode;
};

/*
===============================================================================================

 StorageIndexPage

===============================================================================================
*/

class StorageIndexPage : public StoragePage
{
    typedef InTreeMap<StorageIndexRecord> IndexRecordTree;
public:
    uint32_t            GetSize();
    
    void                Append(ReadBuffer& key, uint32_t index, uint32_t offset);

private:
    IndexRecordTree     indexTree;
};

#endif
