#ifndef STORAGEKEYVALUE_H
#define STORAGEKEYVALUE_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"

/*
===============================================================================

 StorageKeyValue

===============================================================================
*/

class StorageKeyValue
{
public:
    StorageKeyValue();
    ~StorageKeyValue();

    void                    SetKey(ReadBuffer& key, bool copy);
    void                    SetValue(ReadBuffer& value, bool copy);

    ReadBuffer              key;
    ReadBuffer              value;
    
    Buffer*                 keyBuffer;
    Buffer*                 valueBuffer;
    
    InTreeNode<StorageKeyValue> treeNode;
};

inline bool LessThan(StorageKeyValue &a, StorageKeyValue &b)
{
    return ReadBuffer::LessThan(a.key, b.key);
}

#endif
