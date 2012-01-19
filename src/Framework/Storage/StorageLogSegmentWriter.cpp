#include "StorageLogSegmentWriter.h"
#include "StorageLogConsts.h"
#include "StorageEnvironment.h"
#include "System/FileSystem.h"

StorageLogSegmentWriter::StorageLogSegmentWriter()
{
    logCommandID = 1;
    committedLogCommandID = 0;
    filesize = 0;
    fd = INVALID_FD;
}

void StorageLogSegmentWriter::Open(const char* filepath)
{
    filepathBuf.Write(filepath);
    filepathBuf.NullTerminate();

    OpenFile(filepath);
    WriteHeader();

    serializer.Init();
}

void StorageLogSegmentWriter::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
}

bool StorageLogSegmentWriter::IsOpen()
{
    if (fd != INVALID_FD)
        return true;
    else
        return false;
}

bool StorageLogSegmentWriter::HasUncommitted()
{
    return !(committedLogCommandID == logCommandID - 1);
}

uint32_t StorageLogSegmentWriter::GetLogCommandID()
{
    return logCommandID;
}

uint32_t StorageLogSegmentWriter::GetCommittedLogCommandID()
{
    return committedLogCommandID;
}

uint64_t StorageLogSegmentWriter::GetFilesize()
{
    return filesize;
}

uint64_t StorageLogSegmentWriter::GetWriteBufferSize()
{
    return serializer.GetBuffer().GetSize();
}

uint32_t StorageLogSegmentWriter::WriteSet(
 uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value)
{
    serializer.SerializeSet(contextID, shardID, key, value);
    return logCommandID++;
}

uint32_t StorageLogSegmentWriter::WriteDelete(
 uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    serializer.SerializeDelete(contextID, shardID, key);
    return logCommandID++;
}

void StorageLogSegmentWriter::Commit()
{
    serializer.Finalize();
    WriteSection();
    StorageEnvironment::Sync(fd);
    
    filesize += serializer.GetBuffer().GetLength();
    
    serializer.Init();

    committedLogCommandID = logCommandID - 1;
}

void StorageLogSegmentWriter::OpenFile(const char* filepath)
{
    Log_Debug("Opening log segment %s", filepath);

    fd = FS_Open(filepath, FS_CREATE | FS_WRITEONLY | FS_APPEND);
    
    if (fd == INVALID_FD)
    {
        Log_Message("Unable to open log segment file %s on disk.", filepath);
        Log_Message("Free disk space: %s", HUMAN_BYTES(FS_FreeDiskSpace(filepath)));
        Log_Message("This should not happen.");
        Log_Message("Possible causes: not enough disk space, software bug...");
        STOP_FAIL(1);
    }
}

void StorageLogSegmentWriter::WriteHeader()
{
    Buffer      buffer;

    buffer.AppendLittle32(STORAGE_LOGSEGMENT_VERSION);
    buffer.AppendLittle64(0); // for backward compatibility
    
    if (FS_FileWrite(fd, buffer.GetBuffer(), buffer.GetLength()) != (ssize_t) buffer.GetLength())
    {
        Log_Message("Unable to write log segment file header %s to disk.", filepathBuf.GetBuffer());
        Log_Message("Free disk space: %s", HUMAN_BYTES(FS_FreeDiskSpace(filepathBuf.GetBuffer())));
        Log_Message("This should not happen.");
        Log_Message("Possible causes: not enough disk space, software bug...");
        STOP_FAIL(1);
    }

    filesize += buffer.GetLength();

    StorageEnvironment::Sync(fd);
}

void StorageLogSegmentWriter::WriteSection()
{
    ssize_t     ret;
    uint64_t    size;
    uint64_t    offset;
    uint64_t    length;

    length = serializer.GetBuffer().GetLength();

    for (offset = 0; offset < length; offset += size)
    {
        size = MIN(STORAGE_LOGSEGMENT_WRITE_GRANULARITY, length - offset);
        ret = FS_FileWrite(fd, serializer.GetBuffer().GetBuffer() + offset, size);
        if (ret < 0 || (uint64_t) ret != size)
        {
            Log_Message("Unable to write log segment %s to disk.", filepathBuf.GetBuffer());
            Log_Message("Free disk space: %s", HUMAN_BYTES(FS_FreeDiskSpace(filepathBuf.GetBuffer())));
            Log_Message("This should not happen.");
            Log_Message("Possible causes: not enough disk space, software bug...");
            STOP_FAIL(1);
        }        
    }
}
