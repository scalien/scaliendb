#include "StorageRecoveryLog.h"
#include "System/FileSystem.h"

bool StorageRecoveryLog::Open(const char* filename)
{
    fd = FS_Open(filename, FS_READWRITE | FS_CREATE);
    if (fd == INVALID_FD)
        return false;

    return true;
}

bool StorageRecoveryLog::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
    return true;
}

int64_t StorageRecoveryLog::GetFileSize()
{
    return FS_FileSize(fd);
}

bool StorageRecoveryLog::WriteOpHeader(uint32_t op, uint64_t total)
{
    buffer.Append((const char*) ToLittle32(op), sizeof(uint32_t));
    buffer.Append((const char*) ToLittle64(total), sizeof(uint64_t));
    
    return true;
}

bool StorageRecoveryLog::WriteLittle32(uint32_t x)
{
    buffer.Append((const char*) ToLittle32(x), sizeof(uint32_t));
    return true;
}

bool StorageRecoveryLog::WriteLittle64(uint64_t x)
{
    buffer.Append((const char*) ToLittle64(x), sizeof(uint64_t));
    return true;
}

bool StorageRecoveryLog::WriteData(ReadBuffer& data)
{
    buffer.Append(data);
    return true;
}

bool StorageRecoveryLog::WriteUnbufferedData(ReadBuffer& data)
{
    ssize_t     ret;
    
    Flush();
    ret = FS_FileWrite(fd, (const void*) data.GetBuffer(), data.GetLength());
    return true;
}

bool StorageRecoveryLog::Flush()
{
    ssize_t     ret;
    unsigned    length;
    
    length = buffer.GetLength();
    ret = FS_FileWrite(fd, (const void*) buffer.GetBuffer(), buffer.GetLength());

    buffer.Clear();
    if (ret != (ssize_t) length)
        return false;
    
    return true;
}

bool StorageRecoveryLog::Truncate(bool sync)
{
    FS_FileSeek(fd, 0, SEEK_SET);
    FS_FileTruncate(fd, 0); 
    if (sync)
        FS_Sync();
    return true;
}
