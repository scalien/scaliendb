#include "StorageChunkWriter.h"
#include "StorageChunk.h"
#include "System/FileSystem.h"

bool StorageChunkWriter::Write(const char* filename, StorageChunkFile* file_)
{
    file = file_;

    if (fd.Open(filename, FS_READWRITE) == INVALID_FD)
        return false;
    
    if (!WriteEmptyHeaderPage())
        return false;

    if (!WriteDataPages())
        return false;

    if (!WriteIndexPage())
        return false;

    if (file->headerPage.UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    FS_Sync(fd.GetFD());

    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);

    if (!WriteHeaderPage())
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
    
    return true;
}

bool StorageChunkWriter::WriteEmptyHeaderPage()
{
    uint32_t    pageSize;

    pageSize = file->headerPage.GetSize();

    writeBuffer.Allocate(pageSize);
    writeBuffer.SetLength(pageSize);
    writeBuffer.Zero();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteHeaderPage()
{
    file->headerPage.Write(writeBuffer);
    assert(writeBuffer.GetLength() == file->headerPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteDataPages()
{
    unsigned            i;
    StorageDataPage*    dataPage;

    for (i = 0; i < file->numDataPages; i++)
    {
        dataPage = file->dataPages[i];
        dataPage->Write(writeBuffer);
        assert(writeBuffer.GetLength() == dataPage->GetSize());

        if (!WriteBuffer())
            return false;
    }
    
    return true;
}

bool StorageChunkWriter::WriteIndexPage()
{
    file->indexPage.Write(writeBuffer);
    assert(writeBuffer.GetLength() == file->indexPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteBloomPage()
{
    file->bloomPage.Write(writeBuffer);
    assert(writeBuffer.GetLength() == file->bloomPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}
