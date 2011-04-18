#ifndef STORAGEMEMOKEYVALUE_H
#define STORAGEMEMOKEYVALUE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

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

    void            Set(ReadBuffer key, ReadBuffer value);
    void            Delete(ReadBuffer key);
    
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
