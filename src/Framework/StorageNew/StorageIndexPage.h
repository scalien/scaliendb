#ifndef STORAGEINDEXPAGE_H
#define STORAGEINDEXPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"

/*
===============================================================================================

 StorageIndexRecord

===============================================================================================
*/

class StorageIndexRecord
{
    typedef InTreeNode<StorageIndexRecord> TreeNode;

public:
    ReadBuffer      key;
    uint32_t        index;
    uint32_t        offset;
    
    TreeNode        treeNode;
};

#define STORAGE_DEFAULT_INDEX_PAGE_GRAN         (4*1024)

/*
===============================================================================================

 StorageIndexPage

===============================================================================================
*/

class StorageIndexPage : public StoragePage
{
    typedef InTreeMap<StorageIndexRecord> IndexRecordTree;
public:
    StorageIndexPage();

    uint32_t            GetSize();
    
    void                Append(ReadBuffer& key, uint32_t index, uint32_t offset);

    void                Finalize();
    void                Write(Buffer& writeBuffer);

private:
    uint32_t            size;
    uint32_t            length;
    Buffer              buffer;
    IndexRecordTree     indexTree;
};

#endif
