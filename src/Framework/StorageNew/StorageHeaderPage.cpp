#include "StorageHeaderPage.h"

uint32_t StorageHeaderPage::GetSize()
{
    return STORAGE_HEADER_PAGE_SIZE;
}

void StorageHeaderPage::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID_;
}

void StorageHeaderPage::SetLogSegmentID(uint64_t logSegmentID_)
{
    logSegmentID = logSegmentID_;
}

void StorageHeaderPage::SetLogCommandID(uint64_t logCommandID_)
{
    logCommandID = logCommandID_;
}

void StorageHeaderPage::SetNumKeys(uint64_t numKeys_)
{
    numKeys = numKeys_;
}

void StorageHeaderPage::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

void StorageHeaderPage::SetIndexPageOffset(uint64_t indexPageOffset_)
{
    indexPageOffset = indexPageOffset_;
}

void StorageHeaderPage::SetIndexPageSize(uint32_t indexPageSize_)
{
    indexPageSize = indexPageSize_;
}

void StorageHeaderPage::SetBloomPageOffset(uint64_t bloomPageOffset_)
{
    bloomPageOffset = bloomPageOffset_;
}

void StorageHeaderPage::SetBloomPageSize(uint32_t bloomPageSize_)
{
    bloomPageSize = bloomPageSize_;
}
