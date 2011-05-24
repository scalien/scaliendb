#ifndef STORAGELOGSEGMENT_H
#define STORAGELOGSEGMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"
#include "System/IO/FD.h"

#define STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE      (8+8+4) // size + uncomressedLength + CRC
#define STORAGE_LOGSEGMENT_COMMAND_SET          's'
#define STORAGE_LOGSEGMENT_COMMAND_DELETE       'd'

#define STORAGE_LOGSEGMENT_WRITE_GRANULARITY    64*KiB
#define STORAGE_LOGSEGMENT_VERSION              1

class StorageRecovery;
class StorageArchiveLogSegmentJob;

/*
===============================================================================================

 StorageLogSegment

===============================================================================================
*/

class StorageLogSegment
{
    friend class StorageArchiveLogSegmentJob;
    friend class StorageRecovery;

public:
    StorageLogSegment();
    
    bool                Open(Buffer& logPath, uint64_t trackID, uint64_t logSegmentID, uint64_t syncGranularity);
    void                Close();
    void                DeleteFile();

    uint64_t            GetTrackID();
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    
    void                SetOnCommit(Callable* onCommit);

    // Append..() functions return commandID:
    int32_t             AppendSet(uint16_t contextID, uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    int32_t             AppendDelete(uint16_t contextID, uint64_t shardID, ReadBuffer& key);
    void                Undo();

    void                Commit();
    bool                HasUncommitted();
    uint32_t            GetCommitedLogCommandID();

    uint64_t            GetOffset();
    
    StorageLogSegment*  prev;
    StorageLogSegment*  next;

private:
    void                NewRound();

    FD                  fd;
    uint64_t            trackID;
    uint64_t            logSegmentID;
    uint32_t            logCommandID;
    uint32_t            commitedLogCommandID;
    uint64_t            syncGranularity;
    uint64_t            offset;
    uint64_t            lastSyncOffset;
    bool                isCommiting;
    bool                commitStatus;
    Buffer              filename;
    Buffer              writeBuffer;
    Callable*           onCommit;

    bool                writeShardID;
    uint16_t            prevContextID;
    uint64_t            prevShardID;
    unsigned            prevLength;
    
    bool                asyncCommit;
};

#endif
