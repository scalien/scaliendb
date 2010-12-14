#include "StorageHeaderPage.h"

StorageHeaderPage::StorageHeaderPage(StorageFileChunk* owner_)
{
    chunkID = 0;
    logSegmentID = 0;
    logCommandID = 0;
    numKeys = 0;
    useBloomFilter = 0;
    indexPageOffset = 0;
    indexPageSize = 0;
    bloomPageOffset = 0;
    bloomPageSize = 0;
    owner = owner_;
}

uint32_t StorageHeaderPage::GetSize()
{
    return STORAGE_HEADER_PAGE_SIZE;
}

uint64_t StorageHeaderPage::GetChunkID()
{
    return chunkID;
}

uint64_t StorageHeaderPage::GetLogSegmentID()
{
    return logSegmentID;
}

uint32_t StorageHeaderPage::GetLogCommandID()
{
    return logCommandID;
}

uint64_t StorageHeaderPage::GetIndexPageOffset()
{
    return indexPageOffset;
}

uint32_t StorageHeaderPage::GetIndexPageSize()
{
    return indexPageSize;
}

uint64_t StorageHeaderPage::GetBloomPageOffset()
{
    return bloomPageOffset;
}

uint32_t StorageHeaderPage::GetBloomPageSize()
{
    return bloomPageSize;
}

void StorageHeaderPage::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID_;
}

void StorageHeaderPage::SetLogSegmentID(uint64_t logSegmentID_)
{
    logSegmentID = logSegmentID_;
}

void StorageHeaderPage::SetLogCommandID(uint32_t logCommandID_)
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

bool StorageHeaderPage::UseBloomFilter()
{
    return useBloomFilter;
}

void StorageHeaderPage::Write(Buffer& writeBuffer)
{
    Buffer      text;

    text.Allocate(64);
    text.Zero();
    text.Write("ScalienDB Chunk File");
    text.SetLength(text.GetSize());


    writeBuffer.Allocate(STORAGE_HEADER_PAGE_SIZE);
    writeBuffer.SetLength(0);
    writeBuffer.Zero();

    writeBuffer.AppendLittle32(STORAGE_HEADER_PAGE_SIZE);
    writeBuffer.AppendLittle32(0); // dummy for checksum
    writeBuffer.AppendLittle32(STORAGE_HEADER_PAGE_VERSION);
    writeBuffer.Append(text);
    writeBuffer.AppendLittle64(chunkID);
    writeBuffer.AppendLittle64(logSegmentID);
    writeBuffer.AppendLittle32(logCommandID);
    writeBuffer.Appendf("%b", useBloomFilter);
    writeBuffer.AppendLittle64(numKeys);
    writeBuffer.AppendLittle64(indexPageOffset);
    writeBuffer.AppendLittle32(indexPageSize);
    if (useBloomFilter)
    {
        writeBuffer.AppendLittle64(bloomPageOffset);
        writeBuffer.AppendLittle32(bloomPageSize);
    }
    writeBuffer.SetLength(STORAGE_HEADER_PAGE_SIZE);
}

bool StorageHeaderPage::IsLoaded()
{
    return true;
}

void StorageHeaderPage::Unload()
{
}
