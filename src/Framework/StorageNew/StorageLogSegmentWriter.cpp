#include "StorageLogSegmentWriter.h"
#include "System/FileSystem.h"

StorageLogSegmentWriter::StorageLogSegmentWriter()
{
    logSegmentID = 0;
    fd = INVALID_FD;
}

bool StorageLogSegmentWriter::Open(const char* filename, uint64_t logSegmentID_)
{
    unsigned length;
    
    fd = FS_Open(filename, FS_READWRITE);
    if (fd == INVALID_FD)
        return false;

    logSegmentID = logSegmentID_;
    offset = 0;

    writeBuffer.AppendLittle64(logSegmentID);
    length = writeBuffer.GetLength();
    
    if (FS_FileWrite(fd, writeBuffer.GetBuffer(), length) != length)
    {
        FS_FileClose(fd);
        fd = INVALID_FD;
        return false;
    }
    offset += length;

    FS_Sync(fd);
    
    NewRound();
    
    return true;
}

void StorageLogSegmentWriter::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
}

uint64_t StorageLogSegmentWriter::GetLogSegmentID()
{
    return logSegmentID;
}

void StorageLogSegmentWriter::SetOnCommit(Callable& onCommit_)
{
    onCommit = onCommit_;
}

int64_t StorageLogSegmentWriter::AppendSet(uint64_t shardID, ReadBuffer& key, ReadBuffer& value)
{
    assert(fd != INVALID_FD);

    writeBuffer.Appendf("%c", STORAGE_LOGSEGMENT_COMMAND_SET);
    writeBuffer.AppendLittle64(shardID);
    writeBuffer.AppendLittle32(key.GetLength());
    writeBuffer.Append(key);
    writeBuffer.AppendLittle32(value.GetLength());
    writeBuffer.Append(value);

    return true;
}

int64_t StorageLogSegmentWriter::AppendDelete(uint64_t shardID, ReadBuffer& key)
{
    assert(fd != INVALID_FD);

    writeBuffer.Appendf("%c", STORAGE_LOGSEGMENT_COMMAND_DELETE);
    writeBuffer.AppendLittle64(shardID);
    writeBuffer.AppendLittle32(key.GetLength());
    writeBuffer.Append(key);

    return true;
}

void StorageLogSegmentWriter::Commit()
{
    uint32_t    length;
    uint32_t    checksum;
    ReadBuffer  dataPart;
    
    assert(fd != INVALID_FD);

    length = writeBuffer.GetLength();

    assert(length >= STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    
    if (length == STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE)
        return; // empty round

    dataPart.SetBuffer(writeBuffer.GetBuffer() + STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    dataPart.SetLength(length - STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    checksum = dataPart.GetChecksum();
    
    writeBuffer.SetLength(0);
    writeBuffer.AppendLittle32(length);
    writeBuffer.AppendLittle32(checksum);
    writeBuffer.SetLength(length);
    
    if (FS_FileWrite(fd, writeBuffer.GetBuffer(), length) != length)
    {
        FS_FileClose(fd);
        fd = INVALID_FD;
        commitState = Failed;
        return;
    }
    offset += length;

    FS_Sync(fd);
    
    NewRound();
    commitState = Succeeded;
}

uint64_t StorageLogSegmentWriter::GetOffset()
{
    return offset;
}

void StorageLogSegmentWriter::NewRound()
{
    // reserve:
    // 4 bytes for size
    // 4 bytes for CRC
    writeBuffer.Allocate(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    writeBuffer.Zero();
    writeBuffer.SetLength(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);    
}
