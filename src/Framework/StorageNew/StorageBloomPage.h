#ifndef STORAGEBLOOMPAGE_H
#define STORAGEBLOOMPAGE_H

#include "StoragePage.h"
#include "Framework/BloomFilter.h"

class StorageFileChunk;

/*
===============================================================================================

 StorageBloomPage

===============================================================================================
*/

class StorageBloomPage : public StoragePage
{
public:
    StorageBloomPage(StorageFileChunk* owner);

    uint32_t            GetSize();
    
    void                SetNumKeys(uint64_t numKeys);
    void                Add(ReadBuffer key);
    void                Write(Buffer& writeBuffer);
    
    bool                Check(ReadBuffer& key);

    bool                IsLoaded();
    void                Unload();

private:
    uint32_t            RecommendNumBytes(uint32_t numKeys);

    uint32_t            size;
    BloomFilter         bloomFilter;
    StorageFileChunk*   owner;
};

#endif
