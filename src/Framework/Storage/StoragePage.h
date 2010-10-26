#ifndef STORAGEPAGE_H
#define STORAGEPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InTreeMap.h"

#define STORAGE_DATA_PAGE   'd'
#define STORAGE_INDEX_PAGE  'i'

/*
===============================================================================

 StoragePage

===============================================================================
*/

class StoragePage
{
public:
    StoragePage();
    virtual ~StoragePage() {}

    void                    SetStorageFileIndex(uint32_t fileIndex);
    uint32_t                GetStorageFileIndex();

    void                    SetOffset(uint32_t offset);
    uint32_t                GetOffset();

    void                    SetPageSize(uint32_t pageSize);
    uint32_t                GetPageSize();

    void                    SetDirty(bool dirty);
    bool                    IsDirty();
    
    void                    SetNew(bool n);
    bool                    IsNew();
    
    void                    SetDeleted(bool deleted);
    bool                    IsDeleted();

    char                    GetType();

    virtual void            Read(ReadBuffer& buffer)    = 0;
    virtual bool            CheckWrite(Buffer& buffer)  = 0;
    virtual bool            Write(Buffer& buffer)       = 0;

    InTreeNode<StoragePage> treeNode;
    StoragePage*            next;
    StoragePage*            prev;

    Buffer                  buffer;

protected:

    uint32_t                fileIndex;
    uint32_t                offset;
    uint32_t                pageSize;
    bool                    dirty;
    bool                    newPage;
    bool                    deleted;
    char                    type;
};

#endif