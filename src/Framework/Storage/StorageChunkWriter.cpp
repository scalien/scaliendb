#include "StorageChunkWriter.h"
#include "StorageChunk.h"
#include "System/FileSystem.h"

bool StorageChunkWriter::Write(StorageFileChunk* file_)
{
    file = file_;

    if (fd.Open(file->GetFilename().GetBuffer(), FS_CREATE | FS_WRITEONLY | FS_APPEND) == INVALID_FD)
        return false;

    if (!WriteHeaderPage())
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

    fd.Close();

    file->written = true;

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

bool StorageChunkWriter::WriteHeaderPage()
{
    writeBuffer.Clear();
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
        writeBuffer.Clear();
        dataPage->Write(writeBuffer);
        assert(writeBuffer.GetLength() == dataPage->GetSize());

        if (!WriteBuffer())
            return false;
    }
    
    return true;
}

bool StorageChunkWriter::WriteIndexPage()
{
    writeBuffer.Clear();
    file->indexPage->Write(writeBuffer);
    assert(writeBuffer.GetLength() == file->indexPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteBloomPage()
{
    writeBuffer.Clear();
    file->bloomPage->Write(writeBuffer);
    assert(writeBuffer.GetLength() == file->bloomPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}
