#include "StorageChunkWriter.h"
#include "StorageEnvironment.h"
#include "StorageChunk.h"
#include "System/FileSystem.h"
#include "System/Events/EventLoop.h"

bool StorageChunkWriter::Write(StorageEnvironment* env_, StorageFileChunk* file_)
{
    env = env_;
    file = file_;

    file->writeError = true;

    if (fd.Open(file->GetFilename().GetBuffer(), FS_CREATE | FS_WRITEONLY | FS_APPEND | FS_TRUNCATE) == INVALID_FD)
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

    StorageEnvironment::Sync(fd.GetFD());

    fd.Close();

    file->writeError = false;

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
    ASSERT(writeBuffer.GetLength() == file->headerPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteDataPages()
{
    unsigned            i;
    StorageDataPage*    dataPage;

    writeBuffer.Clear();
    for (i = 0; i < file->numDataPages; i++)
    {
        if (env->shuttingDown)
        {
            file->writeError = false;
            return false;
        }

        dataPage = file->dataPages[i];
        dataPage->Serialize(writeBuffer);
        if (writeBuffer.GetLength() > STORAGE_WRITE_GRANULARITY)
        {
            if (!WriteBuffer())
                return false;
            writeBuffer.Clear();
        }
    }

    // write the rest of the buffer to disk
    if (writeBuffer.GetLength() > 0)
    {
        if (!WriteBuffer())
            return false;
        writeBuffer.Clear();
    }

    return true;
}

bool StorageChunkWriter::WriteIndexPage()
{
    writeBuffer.Clear();
    file->indexPage->Write(writeBuffer);
    ASSERT(writeBuffer.GetLength() == file->indexPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkWriter::WriteBloomPage()
{
    writeBuffer.Clear();
    file->bloomPage->Write(writeBuffer);
    ASSERT(writeBuffer.GetLength() == file->bloomPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}
