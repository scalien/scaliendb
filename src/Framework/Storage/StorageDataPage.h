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
public:
    StorageDataPage(StorageFileChunk* owner, uint32_t index);
    ~StorageDataPage();

    uint32_t                GetSize();
    uint32_t                GetMemorySize();
    uint32_t                GetCompressedSize();
    uint32_t                GetPageBufferSize();

    StorageKeyValue*        Get(ReadBuffer& key);

    uint32_t                GetNumKeys();
    uint32_t                GetLength();
    uint32_t                GetIncrement(StorageKeyValue* kv);
    
    void                    Append(StorageKeyValue* kv, bool keysOnly = false);
    void                    Finalize();
    void                    Reset();
    
    StorageFileKeyValue*    First();
    StorageFileKeyValue*    Next(StorageFileKeyValue* it);
    StorageFileKeyValue*    GetIndexedKeyValue(unsigned index);
    StorageFileKeyValue*    LocateKeyValue(ReadBuffer& key, int& cmpres);

    bool                    Read(Buffer& buffer, bool keysOnly = false);
    void                    Write(Buffer& buffer);

    void                    Unload();

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
