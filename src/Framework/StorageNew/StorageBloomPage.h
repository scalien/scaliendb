#ifndef STORAGEBLOOMPAGE_H
#define STORAGEBLOOMPAGE_H

#include "StoragePage.h"
#include "Framework/BloomFilter.h"

/*
===============================================================================================

 StorageBloomPage

 If we want false positive probability p with n key-values in the Bloom filter,
 the filter must use

  m = -1.10628395 * n * log(p)

===============================================================================================
*/

class StorageBloomPage : public StoragePage
{
public:
    void            SetNumKeys(uint64_t numKeys);
    void            Add(StorageKeyValue* kv);
    
private:
    BloomFilter     bloomFilter;
};

#endif
