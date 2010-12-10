#ifndef STORAGELOGSEGMENTWRITER_H
#define STORAGELOGSEGMENTWRITER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"
#include "System/IO/FD.h"

#define STORAGE_LOGSEGMENT_BLOCK_HEAD_SIZE      8
#define STORAGE_LOGSEGMENT_COMMAND_SET          'S'
#define STORAGE_LOGSEGMENT_COMMAND_DELETE       'D'

class StorageLogSegmentWriter
{
public:
    typedef enum CommitState {Succeeded, Failed} CommitState;

    StorageLogSegmentWriter();
    
    bool            Open(const char* filename, uint64_t logSegmentID_);
    void            Close();

    uint64_t        GetLogSegmentID();
    void            SetOnCommit(Callable& onCommit);

    // Append..() functions return commandID:
    int64_t         AppendSet(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    int64_t         AppendDelete(uint64_t shardID, ReadBuffer& key);

    void            Commit();

    uint64_t        GetOffset();

private:
    void            NewRound();

    FD              fd;
    uint64_t        logSegmentID;
    uint64_t        offset;
    CommitState     commitState;
    Buffer          writeBuffer;
    Callable        onCommit;
};

#endif
