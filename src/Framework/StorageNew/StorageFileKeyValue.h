#ifndef STORAGEFILEKEYVALUE_H
#define STORAGEFILEKEYVALUE_H

#include "System/Containers/InTreeMap.h"
#include "System/Buffers/Buffer.h"

#define STORAGE_KEYVALUE_TYPE_SET      's'
#define STORAGE_KEYVALUE_TYPE_DELETE   'd'

class StorageFileKeyValue
{
public:
    typedef InTreeNode<StorageFileKeyValue> TreeNode;
    
    StorageFileKeyValue();

    void            Set(ReadBuffer& key, ReadBuffer& value);
    void            Delete(ReadBuffer& key);
    
    char            GetType();
    ReadBuffer      GetKey();
    ReadBuffer      GetValue();

    TreeNode        treeNode;

private:
    char            type;
    ReadBuffer      key;
    ReadBuffer      value;
};

#endif
