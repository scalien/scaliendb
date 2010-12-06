#ifndef STORAGELOGSEGMENTWRITER_H
#define STORAGELOGSEGMENTWRITER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"

class StorageLogSegmentWriter
{
public:
    bool            Open(const char* filename, uint64_t logSegmentID);

    void            SetOnCommit(Callable& onCommit);
    void            SetOnFinalize(Callable& onFinalize);

    void            AppendDataCommand(uint64_t shardID, uint64_t chunkID, StorageKeyValue* kv);
    void            AppendMetaCommand(StorageMetaCommand& command);

    void            Commit();
    void            Finalize();

    uint64_t        GetLogSize();

private:
    uint64_t        logSegmentID;
    Buffer          appendBuffer;
    Callable        onCommit;
    Callable        onFinalize;
};

#endif
