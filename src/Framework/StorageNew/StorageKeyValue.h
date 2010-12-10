#ifndef STORAGEKEYVALUE_H
#define STORAGEKEYVALUE_H

#include "System/Containers/InTreeMap.h"
#include "System/Buffers/ReadBuffer.h"

#define STORAGE_KEYVALUE_TYPE_SET      's'
#define STORAGE_KEYVALUE_TYPE_DELETE   'd'

class StorageKeyValue
{
public:
    typedef InTreeNode<StorageKeyValue> TreeNode;
    
    void            Set(ReadBuffer& key, ReadBuffer& value);
    void            Delete(ReadBuffer& key);
    
    char            GetType();
    ReadBuffer      GetKey();
    ReadBuffer      GetValue();

    TreeNode        treeNode;
};

#endif
