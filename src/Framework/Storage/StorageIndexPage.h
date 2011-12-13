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
    uint64_t        offset;
    
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

    void                SetOwner(StorageFileChunk* owner);

    uint32_t            GetSize();
    uint32_t            GetMemorySize();
    uint32_t            GetNumDataPages();

    bool                Locate(ReadBuffer& key, uint32_t& index, uint64_t& offset);
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    ReadBuffer          GetMidpoint();
    uint64_t            GetFirstDatapageOffset();
    uint64_t            GetLastDatapageOffset();
    uint32_t            GetOffsetIndex(uint64_t& offset);
    uint64_t            GetIndexOffset(uint32_t index);

    void                Append(ReadBuffer key, uint32_t index, uint64_t offset);
    void                Finalize();

    bool                Read(Buffer& buffer);
    void                Write(Buffer& buffer);

    void                Unload();

private:
    uint32_t            size;
    Buffer              buffer;
    IndexRecordTree     indexTree;
    ReadBuffer          midpoint;
    StorageFileChunk*   owner;
};

#endif
