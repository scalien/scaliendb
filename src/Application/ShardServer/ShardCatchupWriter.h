#ifndef SHARDCATCHUPWRITER_H
#define SHARDCATCHUPWRITER_H

#include "Application/Common/CatchupMessage.h"
#include "Framework/Storage/StorageBulkCursor.h"

class ShardQuorumProcessor; // forward

/*
===============================================================================================

 ShardCatchupWriter

===============================================================================================
*/

class ShardCatchupWriter
{
public:
    ShardCatchupWriter();
    ~ShardCatchupWriter();
    
    void                    Init(ShardQuorumProcessor* quorumProcessor);
    void                    Reset();
    
    bool                    IsActive();
    uint64_t                GetBytesSent();
    uint64_t                GetBytesTotal();
    uint64_t                GetThroughput();

    void                    Begin(CatchupMessage& request);
    void                    Abort();

private:
    void                    SendFirst();
    void                    SendNext();
    void                    SendCommit();
    void                    OnWriteReadyness();
    uint64_t*               NextShard();
    void                    TransformKeyValue(StorageKeyValue* kv, CatchupMessage& msg);

    bool                    isActive;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    uint64_t                paxosID;
    uint64_t                bytesSent;
    uint64_t                bytesTotal;
    uint64_t                startTime;
    ShardQuorumProcessor*   quorumProcessor;
    StorageEnvironment*     environment;
    StorageBulkCursor*      cursor;
    StorageKeyValue*        kv;
};

#endif
