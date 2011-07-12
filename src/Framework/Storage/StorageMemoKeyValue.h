#ifndef STORAGEMEMOKEYVALUE_H
#define STORAGEMEMOKEYVALUE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

class StorageMemoChunk;

/*
===============================================================================================

 StorageMemoKeyValue

===============================================================================================
*/

class StorageMemoKeyValue : public StorageKeyValue
{
public:
    typedef InTreeNode<StorageMemoKeyValue> TreeNode;

    StorageMemoKeyValue();
    ~StorageMemoKeyValue();

    void            Free(StorageMemoChunk* memoChunk);

    void            Set(ReadBuffer key, ReadBuffer value, StorageMemoChunk* memoChunk);
    void            Delete(ReadBuffer key, StorageMemoChunk* memoChunk);
    
    char            GetType();
    ReadBuffer      GetKey() const;
    ReadBuffer      GetValue() const;
    uint32_t        GetLength();

    TreeNode        treeNode;

private:
    char*           buffer;
    uint16_t        keyLength;
    uint32_t        valueLength;
};

#endif
