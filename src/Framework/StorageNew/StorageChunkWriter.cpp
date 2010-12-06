#include "StorageChunkWriter.h"
#include "StorageChunk.h"
#include "System/IO/FileSystem.h"

bool StorageChunkWriter::Write(const char* filename, StorageChunk* chunk)
{
    if (fd.Open(filename, FS_READWRITE) == INVALID_FD)
        return false;
    
    offset = 0;

    indexPage.Clear();
    if (chunk->UseBloomFilter())
    {
        bloomPage.Clear();
        bloomPage.SetNumKeys(chunk->GetNumKeys());
    }

    if (!WriteEmptyHeaderPage())
        return false;

    if (!WriteDataPages(chunk))
        return false;

    if (!WriteIndexPage())
        return false;

    if (chunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    FS_Sync(fd.GetFD());

    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);
    offset = 0;

    if (!WriteHeader())
        return false;

    FS_Sync(fd.GetFD());

    fd.Close();

    return true;
}

bool StorageChunkWriter::WriteBuffer()
{
    ssize_t     writeSize;

    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != writeSize)
        return false;
    
    offset += writeSize;

    return true;
}

bool StorageChunkWriter::WriteEmptyHeaderPage()
{
    uint32_t    pageSize;
    ssize_t     writeSize;

    pageSize = headerPage.GetPageSize();

    writeBuffer.Allocate(pageSize);
    writeBuffer.SetLength(pageSize);
    writeBuffer.Zero();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteHeaderPage(StorageChunk* chunk)
{
    headerPage.SetShardID(chunk->GetShardID());
    headerPage.SetChunkID(chunk->GetChunkID());
    headerPage.SetLogSegmentID(chunk->GetLogSegmentID());
    headerPage.SetNumKeys(chunk->GetNumKeys());

    headerPage.SetVersion(STORAGE_HEADER_PAGE_VERSION);
    headerPage.SetUseBloomFilter(chunk->UseBloomFilter());
    headerPage.SetIndexPageOffset(indexPageOffset);
    headerPage.SetIndexPageSize(indexPageSize);
    headerPage.SetBloomPageOffset(bloomPageOffset);
    headerPage.SetBloomPageSize(bloomPageSize);

    headerPage.Write(writeBuffer);
    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteDataPages(StorageChunk* chunk)
{
    StorageKeyValue*    it;

    dataPage.Clear();
    FOREACH(it, chunk->keyValues)
    {
        if (chunk->UseBloomFilter())
            bloomPage.Add(it);

        if (dataPage.GetNumKeys() == 0)
        {
            dataPage.Append(it);
            indexPage.Append(it);
        }
        else
        {
            if (dataPage.GetLength() + dataPage.GetSizeIncrement(it) <= STORAGE_DEFAULT_DATA_PAGE_SIZE)
            {
                dataPage.Append(it);
            }
            else
            {
                dataPage.Write(writeBuffer);
                if (!WriteBuffer())
                    return false;
                dataPage.Clear();
            }
        }
    }

    // write last datapage
    if (dataPage.GetNumKeys() > 0)
    {
        dataPage.Write(writeBuffer);
        if (!WriteBuffer())
            return false;
    }
    
    return true;
}

bool StorageChunkWriter::WriteIndexPage()
{
    indexPage.Write(writeBuffer);

    indexPageOffset = offset;
    indexPageSize = writeBuffer.GetLength();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteBloomPage()
{
    bloomPage.Write(writeBuffer);

    bloomPageOffset = offset;
    bloomPageSize = writeBuffer.GetLength();

    if (!WriteBuffer())
        return false;

    return true;
}
