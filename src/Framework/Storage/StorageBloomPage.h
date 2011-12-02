#ifndef STORAGEBLOOMPAGE_H
#define STORAGEBLOOMPAGE_H

#include "StoragePage.h"
#include "BloomFilter.h"

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

    void                SetOwner(StorageFileChunk* owner);

    virtual uint32_t    GetSize();
    virtual uint32_t    GetMemorySize();

    void                SetNumKeys(uint64_t numKeys);
    void                Add(ReadBuffer key);
    
    bool                Read(Buffer& buffer);
    virtual void        Write(Buffer& buffer);
    
    bool                Check(ReadBuffer& key);

    virtual void        Unload();

private:
    uint32_t            RecommendNumBytes(uint32_t numKeys);
    uint32_t            RecommendNumHashes(uint32_t numKeys);

    uint32_t            size;
    BloomFilter         bloomFilter;
    StorageFileChunk*   owner;
};

#endif
