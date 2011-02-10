#ifndef STORAGEINDEXPAGE_H
#define STORAGEINDEXPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"

class StorageFileChunk;

/*
===============================================================================================

 StorageIndexRecord

===============================================================================================
*/

class StorageIndexRecord
{
    typedef InTreeNode<StorageIndexRecord> TreeNode;

public:
    Buffer          keyBuffer;
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
    StorageIndexPage(StorageFileChunk* owner);
    ~StorageIndexPage();

    uint32_t            GetSize();
    uint32_t            GetNumDataPages();

    bool                Locate(ReadBuffer& key, uint32_t& index, uint32_t& offset);
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetMidpoint();

    void                Append(ReadBuffer key, uint32_t index, uint32_t offset);
    void                Finalize();

    bool                Read(Buffer& buffer);
    void                Write(Buffer& buffer);

    void                Unload();

private:
    uint32_t            size;
    Buffer              buffer;
    IndexRecordTree     indexTree;
    StorageFileChunk*   owner;
};

#endif
