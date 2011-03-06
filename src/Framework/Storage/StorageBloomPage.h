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

    uint32_t            GetSize();
    
    void                SetNumKeys(uint64_t numKeys);
    void                Add(ReadBuffer key);
    
    bool                Read(Buffer& buffer);
    void                Write(Buffer& buffer);
    
    bool                Check(ReadBuffer& key);

    void                Unload();

private:
    uint32_t            RecommendNumBytes(uint32_t numKeys);
    uint32_t            RecommendNumHashes(uint32_t numKeys);

    uint32_t            size;
    BloomFilter         bloomFilter;
    StorageFileChunk*   owner;
};

#endif
