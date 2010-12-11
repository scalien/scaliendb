#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageKeyValue.h"
#include "StorageFileKeyValue.h"

#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*1024)
#define STORAGE_DEFAULT_DATA_PAGE_GRAN         (4*1024)

/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
    typedef InTreeMap<StorageFileKeyValue> KeyValueTree;
public:
    StorageDataPage();

    uint32_t        GetSize();

    bool            Get(ReadBuffer& key, ReadBuffer& value);

    uint32_t        GetNumKeys();
    uint32_t        GetLength();
    uint32_t        GetIncrement(StorageMemoKeyValue* kv);
    
    void            Append(StorageMemoKeyValue* kv);
    void            Finalize();

    void            Write(Buffer& writeBuffer);

private:
    uint32_t        size;
    Buffer          buffer;
    KeyValueTree    keyValues;
};

#endif
