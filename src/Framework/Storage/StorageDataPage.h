#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileKeyValue.h"

#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*KiB)

class StorageFileChunk;

/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
    friend class StorageFileChunk;
    typedef InTreeMap<StorageFileKeyValue> KeyValueTree;

public:
    StorageDataPage(StorageFileChunk* owner, uint32_t index);
    ~StorageDataPage();

    uint32_t            GetSize();

    StorageKeyValue*    Get(ReadBuffer& key);

    uint32_t            GetNumKeys();
    uint32_t            GetLength();
    uint32_t            GetIncrement(StorageMemoKeyValue* kv);
    
    void                Append(StorageMemoKeyValue* kv);
    void                Finalize();

    bool                Read(Buffer& buffer);
    void                Write(Buffer& buffer);

    void                Unload();

private:
    uint32_t            size;
    uint32_t            index;
    Buffer              buffer;
    KeyValueTree        keyValues;
    StorageFileChunk*   owner;
};

#endif
