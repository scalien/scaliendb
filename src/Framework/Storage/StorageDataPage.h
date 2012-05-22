#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageMemoKeyValue.h"
#include "StorageFileKeyValue.h"

#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*KiB)

class StorageFileChunk;
class StorageDataPage;

/*
===============================================================================================

 StorageDataPageCache

===============================================================================================
*/

class StorageDataPageCache
{
public:
    static void                 SetMaxCacheSize(uint64_t maxCacheSize);
    static uint64_t             GetMaxCacheSize();
    static uint64_t             GetCacheSize();
    static uint64_t             GetMaxUsedSize();
    static uint64_t             GetMaxCachedPageSize();
    static unsigned             GetListLength();
    static void                 Shutdown();
    static StorageDataPage*     Acquire();
    static void                 Release(StorageDataPage* dataPage);
};


/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
private:
    friend class StorageDataPageCache;
    StorageDataPage();

public:
    StorageDataPage(StorageFileChunk* owner, uint32_t index, unsigned bufferSize = STORAGE_DEFAULT_PAGE_GRAN);
    ~StorageDataPage();

    void                    Init(StorageFileChunk* owner_, uint32_t index_, unsigned bufferSize);
    void                    SetOwner(StorageFileChunk* owner);

    uint32_t                GetSize();
    uint32_t                GetMemorySize();
    uint32_t                GetCompressedSize();
    uint32_t                GetPageBufferSize();

    StorageKeyValue*        Get(ReadBuffer& key);

    uint32_t                GetNumKeys();
    uint32_t                GetLength();
    uint32_t                GetIncrement(StorageKeyValue* kv);
    uint32_t                GetIndex();
    
    void                    Append(StorageKeyValue* kv, bool keysOnly = false);
    void                    Finalize();
    void                    Reset();
    
    StorageFileKeyValue*    First();
    StorageFileKeyValue*    Last();
    StorageFileKeyValue*    Next(StorageFileKeyValue* it);
    StorageFileKeyValue*    Prev(StorageFileKeyValue* it);
    StorageFileKeyValue*    GetIndexedKeyValue(unsigned index);
    StorageFileKeyValue*    LocateKeyValue(ReadBuffer& key, int& cmpres);

    bool                    Read(Buffer& buffer, bool keysOnly = false);
    void                    Write(Buffer& buffer);
    // Serialize differs from Write in that it appends to the buffer
    unsigned                Serialize(Buffer& buffer);

    void                    Unload();

    StorageDataPage*        prev;
    StorageDataPage*        next;

private:
    void                    AppendKeyValue(StorageFileKeyValue& kv);

    uint32_t                size;
    uint32_t                compressedSize;
    uint32_t                index;
    Buffer                  buffer;
    Buffer                  keysBuffer;
    Buffer                  valuesBuffer;
    StorageFileChunk*       owner;
    Buffer                  storageFileKeyValueBuffer;
};

#endif
