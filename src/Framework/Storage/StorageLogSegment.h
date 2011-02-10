#ifndef STORAGELOGSEGMENT_H
#define STORAGELOGSEGMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"
#include "System/IO/FD.h"

#define STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE      (4+4+4) // size + uncomressedLength + CRC
#define STORAGE_LOGSEGMENT_COMMAND_SET          's'
#define STORAGE_LOGSEGMENT_COMMAND_DELETE       'd'

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
    
    bool                Open(Buffer& filename, uint64_t logSegmentID_);
    void                Close();
    void                DeleteFile();

    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    
    void                SetOnCommit(Callable* onCommit);

    // Append..() functions return commandID:
    int32_t             AppendSet(uint16_t contextID, uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    int32_t             AppendDelete(uint16_t contextID, uint64_t shardID, ReadBuffer& key);
    void                Undo();

    void                Commit();
    bool                GetCommitStatus();
    bool                HasUncommitted();

    uint64_t            GetOffset();
    
    StorageLogSegment*  prev;
    StorageLogSegment*  next;

private:
    void                NewRound();

    FD                  fd;
    uint64_t            logSegmentID;
    uint32_t            logCommandID;
    uint64_t            offset;
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
