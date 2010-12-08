#ifndef STORAGELOGSEGMENTWRITER_H
#define STORAGELOGSEGMENTWRITER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"

class StorageLogSegmentWriter
{
public:
    bool            Open(const char* filename);
    void            Close();

    uint64_t        GetLogSegmentID();
    void            SetLogSegmentID();
    void            SetOnCommit(Callable& onCommit);

    int64_t         AppendSet(uint64_t shardID, ReadBuffer& key, ReadBuffer& value); // returns commandID
    int64_t         AppendDelete(uint64_t shardID, ReadBuffer& key); // returns commandID

    void            Commit();
    void            Finalize();

    uint64_t        GetSize();

private:
    uint64_t        logSegmentID;
    Buffer          appendBuffer;
    Callable        onCommit;
};

#endif
