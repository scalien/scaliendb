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
    uint32_t        GetSize();
    
    void            SetNumKeys(uint64_t numKeys);
    void            Add(ReadBuffer& key);
    
private:
    BloomFilter     bloomFilter;
};

#endif
