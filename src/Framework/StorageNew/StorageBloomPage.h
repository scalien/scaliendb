#ifndef STORAGEBLOOMPAGE_H
#define STORAGEBLOOMPAGE_H

#include "StoragePage.h"
#include "Framework/BloomFilter.h"

/*
===============================================================================================

 StorageBloomPage

===============================================================================================
*/

class StorageBloomPage : public StoragePage
{
public:
    StorageBloomPage();

    uint32_t        GetSize();
    
    void            SetNumKeys(uint64_t numKeys);
    void            Add(ReadBuffer& key);

    void            Write(Buffer& writeBuffer);
    
private:
    uint32_t        RecommendNumBytes(uint32_t numKeys);

    uint32_t        size;
    BloomFilter     bloomFilter;
};

#endif
