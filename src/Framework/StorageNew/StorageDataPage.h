#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageKeyValue.h"

#define STORAGE_KEYVALUE_FIX_OVERHEAD   1+2+4

/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
    typedef InTreeMap<StorageKeyValue> KeyValueTree;
public:
    StorageDataPage();

    uint32_t        GetSize();
    
    uint32_t        GetNumKeys();

    uint32_t        GetLength();
    uint32_t        GetIncrement(StorageKeyValue* kv);
    
    void            Append(StorageKeyValue* kv);
    void            Finalize();

private:
    KeyValueTree    keyValues;
    uint32_t        length;
};

#endif
