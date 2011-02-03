#include "StorageHeaderPage.h"

StorageHeaderPage::StorageHeaderPage(StorageFileChunk* owner_)
{
    chunkID = 0;
    minLogSegmentID = 0;
    maxLogSegmentID = 0;
    maxLogCommandID = 0;
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

uint64_t StorageHeaderPage::GetMinLogSegmentID()
{
    return minLogSegmentID;
}

uint64_t StorageHeaderPage::GetMaxLogSegmentID()
{
    return maxLogSegmentID;
}

uint32_t StorageHeaderPage::GetMaxLogCommandID()
{
    return maxLogCommandID;
}

uint64_t StorageHeaderPage::GetNumKeys()
{
    return numKeys;
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

ReadBuffer StorageHeaderPage::GetFirstKey()
{
    return ReadBuffer(firstKey);
}

ReadBuffer StorageHeaderPage::GetLastKey()
{
    return ReadBuffer(lastKey);
}

ReadBuffer StorageHeaderPage::GetMidpoint()
{
    return ReadBuffer(midpoint);
}

void StorageHeaderPage::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID_;
}

void StorageHeaderPage::SetMinLogSegmentID(uint64_t minLogSegmentID_)
{
    minLogSegmentID = minLogSegmentID_;
}

void StorageHeaderPage::SetMaxLogSegmentID(uint64_t maxLogSegmentID_)
{
    maxLogSegmentID = maxLogSegmentID_;
}

void StorageHeaderPage::SetMaxLogCommandID(uint32_t maxLogCommandID_)
{
    maxLogCommandID = maxLogCommandID_;
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

void StorageHeaderPage::SetFirstKey(ReadBuffer firstKey_)
{
    firstKey.Write(firstKey_);
}

void StorageHeaderPage::SetLastKey(ReadBuffer lastKey_)
{
    lastKey.Write(lastKey_);
}

void StorageHeaderPage::SetMidpoint(ReadBuffer midpoint_)
{
    midpoint.Write(midpoint_);
}

bool StorageHeaderPage::UseBloomFilter()
{
    return useBloomFilter;
}

bool StorageHeaderPage::Read(Buffer& buffer)
{
    uint32_t        size, checksum, compChecksum, version, firstLen, lastLen, midpointLen;
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


    if (!parse.ReadLittle64(minLogSegmentID))
        return false;
    parse.Advance(8);

    if (!parse.ReadLittle64(maxLogSegmentID))
        return false;
    parse.Advance(8);

    if (!parse.ReadLittle32(maxLogCommandID))
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
    
    if (!parse.ReadLittle32(firstLen))
        return false;
    parse.Advance(4);
    if (parse.GetLength() < firstLen)
        return false;        
    firstKey.Write(parse.GetBuffer(), firstLen);
    parse.Advance(firstLen);

    if (!parse.ReadLittle32(lastLen))
        return false;
    parse.Advance(4);
    if (parse.GetLength() < lastLen)
        return false;        
    lastKey.Write(parse.GetBuffer(), lastLen);
    parse.Advance(lastLen);

    if (!parse.ReadLittle32(midpointLen))
        return false;
    parse.Advance(4);
    if (parse.GetLength() < midpointLen)
        return false;        
    midpoint.Write(parse.GetBuffer(), midpointLen);
    parse.Advance(midpointLen);
    
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
    writeBuffer.AppendLittle64(minLogSegmentID);
    writeBuffer.AppendLittle64(maxLogSegmentID);
    writeBuffer.AppendLittle32(maxLogCommandID);
    writeBuffer.Appendf("%b", useBloomFilter);
    writeBuffer.AppendLittle64(numKeys);
    writeBuffer.AppendLittle64(indexPageOffset);
    writeBuffer.AppendLittle32(indexPageSize);
    if (useBloomFilter)
    {
        writeBuffer.AppendLittle64(bloomPageOffset);
        writeBuffer.AppendLittle32(bloomPageSize);
    }
    writeBuffer.AppendLittle32(firstKey.GetLength());
    writeBuffer.Append(firstKey);
    writeBuffer.AppendLittle32(lastKey.GetLength());
    writeBuffer.Append(lastKey);
    writeBuffer.AppendLittle32(midpoint.GetLength());
    writeBuffer.Append(midpoint);
    writeBuffer.SetLength(STORAGE_HEADER_PAGE_SIZE);
    dataPart.Wrap(writeBuffer.GetBuffer() + 8, writeBuffer.GetLength() - 8);
    checksum = dataPart.GetChecksum();
    writeBuffer.SetLength(4);
    writeBuffer.AppendLittle32(checksum);
    
    writeBuffer.SetLength(STORAGE_HEADER_PAGE_SIZE);
}

void StorageHeaderPage::Unload()
{
    firstKey.Reset();
    lastKey.Reset();
}
