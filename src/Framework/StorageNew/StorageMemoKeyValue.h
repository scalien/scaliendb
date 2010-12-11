#ifndef STORAGEKEYMEMOVALUE_H
#define STORAGEKEYMEMOVALUE_H

#include "System/Containers/InTreeMap.h"
#include "System/Buffers/Buffer.h"

#define STORAGE_KEYVALUE_TYPE_SET      's'
#define STORAGE_KEYVALUE_TYPE_DELETE   'd'

class StorageMemoKeyValue
{
public:
    typedef InTreeNode<StorageMemoKeyValue> TreeNode;

    StorageMemoKeyValue();

    void            Set(ReadBuffer& key, ReadBuffer& value);
    void            Delete(ReadBuffer& key);
    
    char            GetType();
    ReadBuffer      GetKey();
    ReadBuffer      GetValue();
    uint32_t        GetLength();

    TreeNode        treeNode;

private:
    Buffer*         key;
    Buffer*         value;
};

#endif
