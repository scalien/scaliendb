#include "StorageRecoveryLog.h"
#include "System/FileSystem.h"

bool StorageRecoveryLog::Open(const char* filename)
{
    name.Write(filename);
    name.NullTerminate();
    
    fd = FS_Open(filename, FS_READWRITE | FS_CREATE);
    if (fd == INVALID_FD)
        return false;

    length = FS_FileSize(fd);

    return true;
}

bool StorageRecoveryLog::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
    
    // if recovery file is closed then we don't need it anymore
    FS_Delete(name.GetBuffer());
    
    return true;
}

int64_t StorageRecoveryLog::GetFileSize()
{
    return FS_FileSize(fd);
}

bool StorageRecoveryLog::WriteOp(uint32_t op, uint32_t dataSize, Buffer& buffer)
{
    Buffer      tmp;
    ssize_t     ret;
    
    tmp.AppendLittle32(op);
    tmp.AppendLittle32(dataSize);
    
    ret = FS_FileWrite(fd, (const void*) tmp.GetBuffer(), tmp.GetLength());
    if (ret < 0)
        return false;
    
    ret = FS_FileWrite(fd, (const void*) buffer.GetBuffer(), buffer.GetLength());
    if (ret < 0)
        return false;
    
    if (dataSize > buffer.GetLength())
        FS_FileSeek(fd, dataSize - buffer.GetLength(), FS_SEEK_CUR);
    
    return true;
}

bool StorageRecoveryLog::WriteDone()
{
    Buffer  empty;

    return WriteOp(RECOVERY_OP_DONE, 0, empty);
}

bool StorageRecoveryLog::Truncate(bool sync)
{
    FS_FileSeek(fd, 0, FS_SEEK_SET);
    FS_FileTruncate(fd, 0); 
    if (sync)
        FS_Sync();
    return true;
}

bool StorageRecoveryLog::Check(uint32_t /*length*/)
{
    // TODO:
    return true;
}

bool StorageRecoveryLog::PerformRecovery(Callable callback_)
{
    int64_t     required;
    Buffer      buffer;
    char*       p;
    uint64_t    pos;

    callback = callback_;
    FS_FileSeek(fd, 0, FS_SEEK_SET);

    pos = 0;
    failed = false;
    while (!failed)
    {
        required = sizeof(op) + sizeof(dataSize);
        if (FS_FileRead(fd, (void*) buffer.GetBuffer(), required) != required)
            return false;
            
        p = buffer.GetBuffer();
        op = FromLittle32(*((uint32_t*) p));
        p += sizeof(op);
        dataSize = FromLittle32(*((uint32_t*) p));
        pos += required + dataSize;

        Call(callback);
        if (failed)
            return false;

        if (op == RECOVERY_OP_DONE)
            break;

        if (pos == length)
            return false;

        if (FS_FileSeek(fd, pos, FS_SEEK_SET) < 0)
            return false;
    }
    
    return true;
}

uint32_t StorageRecoveryLog::GetOp()
{
    return op;
}

uint32_t StorageRecoveryLog::GetDataSize()
{
    return dataSize;
}

int32_t StorageRecoveryLog::ReadOpData(Buffer& buffer)
{
    uint64_t    nread;
    
    nread = dataSize;
    if (buffer.GetSize() < dataSize)
        nread = buffer.GetSize();
    
    if (FS_FileRead(fd, (void*) buffer.GetBuffer(), nread) != (ssize_t) nread)
        return -1;
    
    buffer.SetLength(nread);
    dataSize -= nread;
    return (int32_t) dataSize;
}

void StorageRecoveryLog::Fail()
{
    failed = true;
}
