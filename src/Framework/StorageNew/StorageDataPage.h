#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageKeyValue.h"

#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*1024)
#define STORAGE_DEFAULT_DATA_PAGE_GRAN         (4*1024)

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

    void            Write(Buffer& writeBuffer);

private:
    uint32_t        size;
    uint32_t        length;
    Buffer          buffer;
    KeyValueTree    keyValues;
};

#endif
