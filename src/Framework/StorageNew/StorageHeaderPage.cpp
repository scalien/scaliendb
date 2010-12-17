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

bool StorageHeaderPage::Read(Buffer& buffer)
{
    uint32_t        size, checksum, compChecksum, version;
    ReadBuffer      parse, dataPart;
    
    parse.Wrap(buffer);
    
    // size
    if (!parse.ReadLittle32(size))
        return false;
    if (size != STORAGE_HEADER_PAGE_SIZE)
        return false;
    if (buffer.GetLength() != size)
        return false;
    parse.Advance(4);

    // checksum
    if (!parse.ReadLittle32(checksum))
        return false;
    dataPart.Wrap(buffer.GetBuffer() + 8, buffer.GetLength() - 8);
    compChecksum = dataPart.GetChecksum();
    if (compChecksum != checksum)
        return false;
    parse.Advance(4);

    if (!parse.ReadLittle32(version))
        return false;
    if (version != STORAGE_HEADER_PAGE_VERSION)
        return false;
    parse.Advance(4);

    parse.Advance(64); // text

    if (!parse.ReadLittle64(chunkID))
        return false;
    parse.Advance(8);


    if (!parse.ReadLittle64(logSegmentID))
        return false;
    parse.Advance(8);

    if (!parse.ReadLittle32(logCommandID))
        return false;
    parse.Advance(4);

    parse.Advance(parse.Readf("%b", &useBloomFilter));

    if (!parse.ReadLittle64(numKeys))
        return false;
    parse.Advance(8);

    if (!parse.ReadLittle64(indexPageOffset))
        return false;
    parse.Advance(8);

    if (!parse.ReadLittle32(indexPageSize))
        return false;
    parse.Advance(4);

    if (useBloomFilter)
    {
        if (!parse.ReadLittle64(bloomPageOffset))
            return false;
        parse.Advance(8);

        if (!parse.ReadLittle32(bloomPageSize))
            return false;
        parse.Advance(4);
    }
    
    return true;
}

void StorageHeaderPage::Write(Buffer& writeBuffer)
{
    uint32_t    checksum;
    Buffer      text;
    ReadBuffer  dataPart;

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
    dataPart.Wrap(writeBuffer.GetBuffer() + 8, writeBuffer.GetLength() - 8);
    checksum = dataPart.GetChecksum();
    writeBuffer.SetLength(4);
    writeBuffer.AppendLittle32(checksum);
    
    writeBuffer.SetLength(STORAGE_HEADER_PAGE_SIZE);
}

void StorageHeaderPage::Unload()
{
}
