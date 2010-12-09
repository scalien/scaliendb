#ifndef STORAGEKEYVALUE_H
#define STORAGEKEYVALUE_H

#include "System/Containers/InTreeMap.h"

class StorageKeyValue
{
    typedef InTreeNode<StorageKeyValue> TreeNode;
    
public:

    TreeNode        treeNode;
};

#endif
