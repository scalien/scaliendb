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
    
    size = RecommendNumBytes(numKeys);
    
    numBytes = size - 8; // for pageSize + CRC
    
    bloomFilter.SetSize(numBytes);
}

void StorageBloomPage::Add(ReadBuffer key)
{
    bloomFilter.Add(key);
}

uint32_t StorageBloomPage::RecommendNumBytes(uint32_t numKeys)
{
    uint32_t n, k, m, i;
    
    // how many BYTES would we need per the standard formula
    // with p = 0.1 false positive probability
    m = numKeys * 0.599066;

    // find the smallest 2^i K bigger than m
    // but no bigger than 256K
    // eg. if numKeys = 10.000
    //              m = 5.990
    // than the loop below will stop at
    //              i = 3
    //              n = 8
    //              k = 8K
    // so we recommend 8K bytes (8K * 8 = 64K bits)
    n = 1;
    for (i = 1; i < 8; i++)
    {
        n = 2 * n;
        k = n * 1024;
        if (m < k)
            return m;        
    }

    return m;
}

void StorageBloomPage::Write(Buffer& writeBuffer)
{
    uint32_t checksum;

    writeBuffer.Allocate(size);
    writeBuffer.Zero();

    checksum = bloomFilter.GetBuffer().GetChecksum();

    writeBuffer.AppendLittle32(size);
    writeBuffer.AppendLittle32(checksum);
    writeBuffer.Append(bloomFilter.GetBuffer());
}

bool StorageBloomPage::Check(ReadBuffer& key)
{
    return bloomFilter.Check(key);
}
