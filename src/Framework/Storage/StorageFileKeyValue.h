#ifndef STORAGEFILEKEYVALUE_H
#define STORAGEFILEKEYVALUE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageKeyValue.h"

/*
===============================================================================================

 StorageFileKeyValue

===============================================================================================
*/

class StorageFileKeyValue : public StorageKeyValue
{
public:
    typedef InTreeNode<StorageFileKeyValue> TreeNode;
    
    StorageFileKeyValue();

    void            Set(ReadBuffer key, ReadBuffer value);
    void            Delete(ReadBuffer key);
    
    char            GetType();
    ReadBuffer      GetKey();
    ReadBuffer      GetValue();

    TreeNode        treeNode;

    uint32_t        GetNextIndex();
    void            SetNextIndex(uint32_t nextIndex);

private:
    char            type;
    ReadBuffer      key;
    ReadBuffer      value;
    uint32_t        nextIndex;
};

#endif
