#ifndef STORAGEINDEXPAGE_H
#define STORAGEINDEXPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"

#define STORAGE_DEFAULT_INDEX_PAGE_GRAN         (4*1024)

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

    bool                Locate(ReadBuffer& key, uint32_t& index, uint32_t& offset);

    void                Append(ReadBuffer key, uint32_t index, uint32_t offset);
    void                Finalize();
    void                Write(Buffer& writeBuffer);

private:
    uint32_t            size;
    Buffer              buffer;
    IndexRecordTree     indexTree;
};

#endif
