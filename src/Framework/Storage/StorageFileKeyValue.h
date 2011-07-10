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
    StorageFileKeyValue();

    void            Set(ReadBuffer key, ReadBuffer value);
    void            Delete(ReadBuffer key);
    
    char            GetType();
    ReadBuffer      GetKey() const;
    ReadBuffer      GetValue() const;

private:
    char            type;
    ReadBuffer      key;
    ReadBuffer      value;
};

#endif
