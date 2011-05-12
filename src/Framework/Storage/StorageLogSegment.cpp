#include "StorageLogSegment.h"
#include "System/FileSystem.h"
#include "System/IO/IOProcessor.h"
#include "System/Stopwatch.h"

StorageLogSegment::StorageLogSegment()
{
    prev = next = this;
    trackID = 0;
    logSegmentID = 0;
    fd = INVALID_FD;
    logCommandID = 1;
    commitedLogCommandID = 0;
    prevContextID = 0;
    prevShardID = 0;
    writeShardID = true;
    asyncCommit = false;
}

#define Log_DebugLong(sw, ...)  \
    if (sw.Elapsed() > 1000)    \
    do {                        \
        Log_Debug(__VA_ARGS__); \
        sw.Reset();             \
    } while (0)

bool StorageLogSegment::Open(Buffer& logPath, uint64_t trackID_, uint64_t logSegmentID_, uint64_t syncGranularity_)
{
    unsigned    length;
    Stopwatch   sw;

    trackID = trackID_;
    logSegmentID = logSegmentID_;
    syncGranularity = syncGranularity_;
    offset = 0;
    lastSyncOffset = 0;
    
    sw.Start();
    
    filename.Write(logPath);
    filename.Appendf("log.%020U.%020U", trackID, logSegmentID);
    filename.NullTerminate();
    fd = FS_Open(filename.GetBuffer(), FS_CREATE | FS_WRITEONLY | FS_APPEND);
    if (fd == INVALID_FD)
        return false;

    sw.Stop();
    
    Log_DebugLong(sw, "log segment Open() took %U msec", (uint64_t) sw.Elapsed());

    sw.Start();
    writeBuffer.AppendLittle64(logSegmentID);
    length = writeBuffer.GetLength();
    
    if (FS_FileWrite(fd, writeBuffer.GetBuffer(), length) != (ssize_t) length)
    {
        FS_FileClose(fd);
        fd = INVALID_FD;
        return false;
    }
    offset += length;

    sw.Stop();

    Log_DebugLong(sw, "log segment Open() took %U msec, length = %u", (uint64_t) sw.Elapsed(), length);

    sw.Start();
    FS_Sync(fd);
    sw.Stop();

    Log_DebugLong(sw, "log segment Open() took %U msec", (uint64_t) sw.Elapsed());
    
    NewRound();
    
    Log_Debug("Opening log segment %U", logSegmentID);
        
    return true;
}

void StorageLogSegment::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
    writeBuffer.Reset();
}

void StorageLogSegment::DeleteFile()
{
    FS_Delete(filename.GetBuffer());
}

uint64_t StorageLogSegment::GetTrackID()
{
    return trackID;
}

uint64_t StorageLogSegment::GetLogSegmentID()
{
    return logSegmentID;
}

uint32_t StorageLogSegment::GetLogCommandID()
{
    return logCommandID;
}

void StorageLogSegment::SetOnCommit(Callable* onCommit_)
{
    onCommit = onCommit_;
    asyncCommit = true;
}

int32_t StorageLogSegment::AppendSet(uint16_t contextID, uint64_t shardID,
 ReadBuffer& key, ReadBuffer& value)
{
    ASSERT(fd != INVALID_FD);

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
    writeBuffer.AppendLittle16(key.GetLength());
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
    ASSERT(fd != INVALID_FD);

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
    writeBuffer.AppendLittle16(key.GetLength());
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
    uint32_t    checksum;
    uint64_t    length;
    uint64_t    writeSize;
    uint64_t    writeOffset;
    ssize_t     ret;
    
    commitStatus = true;

    ASSERT(fd != INVALID_FD);

    length = writeBuffer.GetLength();

    ASSERT(length >= STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    
    if (length == STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE)
        return; // empty round

    checksum = 0;

    writeBuffer.SetLength(0);
    writeBuffer.AppendLittle64(length);
    writeBuffer.AppendLittle64(length - STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    writeBuffer.AppendLittle32(checksum);
    writeBuffer.SetLength(length);
    
    for (writeOffset = 0; writeOffset < length; writeOffset += writeSize)
    {
        writeSize = MIN(STORAGE_LOGSEGMENT_WRITE_GRANULARITY, length - writeOffset);
        ret = FS_FileWrite(fd, writeBuffer.GetBuffer() + writeOffset, writeSize);
        if (ret < 0 || (uint64_t) ret != writeSize)
        {
            FS_FileClose(fd);
            fd = INVALID_FD;
            commitStatus = false;
            return;
        }
        
        if (syncGranularity > 0 && writeOffset - lastSyncOffset > syncGranularity)
        {
            FS_Sync(fd);
            lastSyncOffset = writeOffset;
        }
    }

    offset += length;

    FS_Sync(fd);
    
    NewRound();
    commitedLogCommandID = logCommandID - 1;
    
    if (asyncCommit)
        IOProcessor::Complete(onCommit);
}

bool StorageLogSegment::HasUncommitted()
{
    return (writeBuffer.GetLength() > STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
}

uint32_t StorageLogSegment::GetCommitedLogCommandID()
{
    return commitedLogCommandID;
}

uint64_t StorageLogSegment::GetOffset()
{
    return offset;
}

void StorageLogSegment::NewRound()
{
    // reserve:
    // 4 bytes for size
    // 4 bytes for uncompressedLength
    // 4 bytes for CRC
    writeBuffer.Allocate(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);
    writeBuffer.Zero();
    writeBuffer.SetLength(STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE);    

    prevLength = writeBuffer.GetLength();
}
