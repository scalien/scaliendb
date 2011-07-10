#include "StorageBloomPage.h"
#include "StorageFileChunk.h"

#define STORAGE_BLOOMPAGE_HEADER_SIZE   8

StorageBloomPage::StorageBloomPage(StorageFileChunk* owner_)
{
    size = 0;
    owner = owner_;
}

uint32_t StorageBloomPage::GetSize()
{
    return size;
}

uint32_t StorageBloomPage::GetMemorySize()
{
    return size;
}

void StorageBloomPage::SetNumKeys(uint64_t numKeys)
{
    uint32_t    numBytes;
    
    size = RecommendNumBytes(numKeys);
    
    numBytes = size - STORAGE_BLOOMPAGE_HEADER_SIZE; // for pageSize + CRC
    
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
    m = (uint32_t) ceil(numKeys * 0.599066);

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
    for (i = 0; i < 8; i++)
    {
        n = 2 * n;
        k = n * KiB;
        if (m < k)
        {
            m = k;
            break;
        }
    }
    
    if (m < STORAGE_DEFAULT_PAGE_GRAN)
        m = STORAGE_DEFAULT_PAGE_GRAN;
    
    if (m > 256*KiB)
        m = 256*KiB;
    
    return m;
}

bool StorageBloomPage::Read(Buffer& buffer)
{
    ReadBuffer  dataPart, parse;
    uint32_t    size, checksum, compChecksum;
    
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < STORAGE_BLOOMPAGE_HEADER_SIZE)
        goto Fail;
    if (buffer.GetLength() != size)
        goto Fail;
    parse.Advance(4);

    // checksum
    parse.ReadLittle32(checksum);
    dataPart.Wrap(buffer.GetBuffer() + STORAGE_BLOOMPAGE_HEADER_SIZE, 
     buffer.GetLength() - STORAGE_BLOOMPAGE_HEADER_SIZE);
    compChecksum = dataPart.GetChecksum();
    if (compChecksum != checksum)
        goto Fail;
    parse.Advance(4);

    bloomFilter.SetBuffer(parse);
    this->size = size;
    return true;

Fail:
    bloomFilter.GetBuffer().Reset();
    return false;
}

void StorageBloomPage::Write(Buffer& buffer)
{
    uint32_t    checksum;

    buffer.Allocate(size);
    buffer.Zero();

    checksum = bloomFilter.GetBuffer().GetChecksum();

    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);
    buffer.Append(bloomFilter.GetBuffer());
}

bool StorageBloomPage::Check(ReadBuffer& key)
{
    return bloomFilter.Check(key);
}

void StorageBloomPage::Unload()
{
    bloomFilter.Reset();
    owner->OnBloomPageEvicted();
}
