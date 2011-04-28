#include "StorageChunkWriter.h"
#include "StorageEnvironment.h"
#include "StorageChunk.h"
#include "System/FileSystem.h"
#include "System/Events/EventLoop.h"

bool StorageChunkWriter::Write(StorageEnvironment* env_, StorageFileChunk* file_)
{
    env = env_;
    file = file_;

    if (fd.Open(file->GetFilename().GetBuffer(), FS_CREATE | FS_WRITEONLY | FS_APPEND) == INVALID_FD)
        return false;
    
    FS_FileTruncate(fd.GetFD(), 0);

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

    for (i = 0; i < file->numDataPages; i++)
    {
        if (env->shuttingDown)
            return false;
        
        while (env->yieldThreads)
        {
            Log_Trace("Yielding...");
            MSleep(YIELD_TIME);
        }
        
        dataPage = file->dataPages[i];
        writeBuffer.Clear();
        dataPage->Write(writeBuffer);
        //ASSERT(writeBuffer.GetLength() == dataPage->GetSize());

        if (!WriteBuffer())
            return false;
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
