#include "StorageBloomPage.h"

uint32_t StorageBloomPage::GetSize()
{
}

void StorageBloomPage::SetNumKeys(uint64_t numKeys)
{
    uint32_t numBytes;
    
    numBytes = BloomFilter::RecommendNumBytes(numKeys);
    
    numBytes -= 4; // for the CRC
    
    bloomFilter.SetSize(numBytes);
}

void StorageBloomPage::Add(ReadBuffer& key)
{
    bloomFilter.Add(key);
}
