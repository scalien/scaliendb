#ifndef SHARDCATCHUPWRITER_H
#define SHARDCATCHUPWRITER_H

#include "System/Containers/List.h"
#include "System/Events/Countdown.h"
#include "Framework/Storage/StorageBulkCursor.h"
#include "Application/Common/CatchupMessage.h"
#include "Application/Common/ContextTransport.h"


class ShardQuorumProcessor; // forward
class ShardMessage;         // forward

#define SHARD_CATCHUP_WRITER_DELAY  60*1000 // msec
#define SHARD_CATCHUP_WRITER_GRAN   10*KiB

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
    bool                    IsDone();
    uint64_t                GetBytesSent();
    uint64_t                GetBytesTotal();
    uint64_t                GetThroughput();

    void                    Begin(CatchupMessage& request);
    void                    Abort();

    void                    OnShardMessage(uint64_t paxosID, uint64_t commandID, uint64_t shardID, ShardMessage& shardMessage);
    void                    OnBlockShard();
    void                    OnTryCommit();
    void                    SendCommit();

private:
    void                    SendFirst();
    void                    SendNext();
    void                    OnWriteReadyness();
    uint64_t*               NextShard();
    void                    TransformKeyValue(StorageKeyValue* kv, CatchupMessage& msg);
    void                    OnTimeout();

    bool                    isActive;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    uint64_t                bytesSent;
    uint64_t                startTime;
    uint64_t                prevBytesSent;
    List<uint64_t>          forwardShardIDs;
    ShardQuorumProcessor*   quorumProcessor;
    StorageEnvironment*     environment;
    StorageBulkCursor*      cursor;
    StorageKeyValue*        kv;
    Countdown               onTimeout;
    YieldTimer              onTryCommit;
    WriteReadyness          writeReadyness;
};

#endif
