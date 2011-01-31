#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileKeyValue.h"

//#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*KiB)
//#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (4*KiB)
#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (16*KiB)

class StorageFileChunk;

/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
public:
    StorageDataPage(StorageFileChunk* owner, uint32_t index);
    ~StorageDataPage();

    uint32_t                GetSize();

    StorageKeyValue*        Get(ReadBuffer& key);

    uint32_t                GetNumKeys();
    uint32_t                GetLength();
    uint32_t                GetIncrement(StorageKeyValue* kv);
    
    void                    Append(StorageKeyValue* kv);
    void                    Finalize();
    void                    Reset();
    
    StorageFileKeyValue*    First();
    StorageFileKeyValue*    Next(StorageFileKeyValue* it);
    StorageFileKeyValue*    GetIndexedKeyValue(unsigned index);

    bool                    Read(Buffer& buffer);
    void                    Write(Buffer& buffer);

    void                    Unload();

private:
    StorageFileKeyValue*    LocateKeyValue(ReadBuffer& key, int& cmpres);
    void                    AppendKeyValue(StorageFileKeyValue* kv);

    uint32_t                size;
    uint32_t                index;
    Buffer                  buffer;
    StorageFileChunk*       owner;
    Buffer                  keyValueIndexBuffer;
};

#endif
