#ifndef STORAGELOGSEGMENTWRITER_H
#define STORAGELOGSEGMENTWRITER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"
#include "System/IO/FD.h"

#define STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE      (4+4) // size + CRC
#define STORAGE_LOGSEGMENT_COMMAND_SET          'S'
#define STORAGE_LOGSEGMENT_COMMAND_DELETE       'D'

/*
===============================================================================================

 StorageLogSegmentWriter

===============================================================================================
*/

class StorageLogSegmentWriter
{
public:
    StorageLogSegmentWriter();
    
    bool            Open(Buffer& filename, uint64_t logSegmentID_);
    void            Close();

    uint64_t        GetLogSegmentID();
    void            SetOnCommit(Callable& onCommit);

    // Append..() functions return commandID:
    int32_t         AppendSet(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    int32_t         AppendDelete(uint64_t shardID, ReadBuffer& key);
    void            Undo();

    void            Commit();
    bool            GetCommitStatus();

    uint64_t        GetOffset();

private:
    void            NewRound();

    FD              fd;
    uint64_t        logSegmentID;
    uint32_t        logCommandID;
    uint64_t        offset;
    bool            commitStatus;
    Buffer          writeBuffer;
    Callable        onCommit;

    unsigned        prevLength;
};

#endif
