#include "StorageLogSegment.h"
#include "System/FileSystem.h"
#include "System/IO/IOProcessor.h"

StorageLogSegment::StorageLogSegment()
{
    logSegmentID = 0;
    fd = INVALID_FD;
    logCommandID = 1;
    prevContextID = 0;
    prevShardID = 0;
    writeShardID = true;
    asyncCommit = false;
}

bool StorageLogSegment::Open(Buffer& filename, uint64_t logSegmentID_)
{
    unsigned length;
    
    filename.NullTerminate();
    fd = FS_Open(filename.GetBuffer(), FS_CREATE | FS_READWRITE);
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
    
    Log_Message("Opening log segment %U", logSegmentID);
    
    return true;
}

void StorageLogSegment::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
}

uint64_t StorageLogSegment::GetLogSegmentID()
{
    return logSegmentID;
}

void StorageLogSegment::SetOnCommit(Callable* onCommit_)
{
    onCommit = onCommit_;
    asyncCommit = true;
}

int32_t StorageLogSegment::AppendSet(uint16_t contextID, uint64_t shardID,
 ReadBuffer& key, ReadBuffer& value)
{
    assert(fd != INVALID_FD);

    prevLength = writeBuffer.GetLength();

    writeBuffer.Appendf("%c", STORAGE_LOGSEGMENT_COMMAND_SET);
    if (!writeShardID && contextID == prevContextID && shardID == prevShardID)
    {
        writeBuffer.Appendf("%b", true); // use previous shardID
    }
    else
    {
        writeBuffer.Appendf("%b", false);
        writeBuffer.AppendLittle16(contextID);
        writeBuffer.AppendLittle64(shardID);
    }
    writeBuffer.AppendLittle32(key.GetLength());
    writeBuffer.Append(key);
    writeBuffer.AppendLittle32(value.GetLength());
    writeBuffer.Append(value);

    writeShardID = false;
    prevContextID = contextID;
    prevShardID = shardID;
    return logCommandID++;
}

int32_t StorageLogSegment::AppendDelete(uint16_t contextID, uint64_t shardID, ReadBuffer& key)
{
    assert(fd != INVALID_FD);

    prevLength = writeBuffer.GetLength();

    writeBuffer.Appendf("%c", STORAGE_LOGSEGMENT_COMMAND_DELETE);

    if (!writeShardID && contextID == prevContextID && shardID == prevShardID)
    {
        writeBuffer.Appendf("%b", true); // use previous shardID
    }
    else
    {
        writeBuffer.Appendf("%b", false);
        writeBuffer.AppendLittle16(contextID);
        writeBuffer.AppendLittle64(shardID);
    }
    writeBuffer.AppendLittle32(key.GetLength());
    writeBuffer.Append(key);

    writeShardID = false;
    prevContextID = contextID;
    prevShardID = shardID;
    return logCommandID++;
}

void StorageLogSegment::Undo()
{
    writeBuffer.SetLength(prevLength);
    logCommandID--;
    writeShardID = true;
}

void StorageLogSegment::Commit()
{
    uint32_t    length;
    uint32_t    checksum;
    ReadBuffer  dataPart;
    
    commitStatus = true;

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
        commitStatus = false;
        return;
    }
    offset += length;

    FS_Sync(fd);
    
    NewRound();
    
    if (asyncCommit)
        IOProcessor::Complete(onCommit);
}

bool StorageLogSegment::GetCommitStatus()
{
    return commitStatus;
}

uint64_t StorageLogSegment::GetOffset()
{
    return offset;
}

void StorageLogSegment::NewRound()
{
    // reserve:
    // 4 bytes for size
    // 4 bytes for CRC
    writeBuffer.Allocate(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    writeBuffer.Zero();
    writeBuffer.SetLength(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);    

    prevLength = writeBuffer.GetLength();
}
