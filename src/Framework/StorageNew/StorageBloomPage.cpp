#include "StorageBloomPage.h"

StorageBloomPage::StorageBloomPage()
{
    size = 0;
}

uint32_t StorageBloomPage::GetSize()
{
    return size;
}

void StorageBloomPage::SetNumKeys(uint64_t numKeys)
{
    uint32_t numBytes;
    
    size = BloomFilter::RecommendNumBytes(numKeys);
    
    numBytes = size - 4; // for the CRC
    
    bloomFilter.SetSize(numBytes);
}

void StorageBloomPage::Add(ReadBuffer& key)
{
    bloomFilter.Add(key);
}
