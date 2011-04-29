#ifndef SHARDMIGRATIONWRITER_H
#define SHARDMIGRATIONWRITER_H

#include "System/Events/Countdown.h"
#include "Framework/Storage/StorageBulkCursor.h"
#include "Framework/Storage/StorageAsyncBulkCursor.h"
#include "Application/Common/ClusterMessage.h"

class ShardServer;
class ShardQuorumProcessor;

#define SHARD_MIGRATION_WRITER_DELAY  10*1000 // msec
#define SHARD_MIGRATION_WRITER_GRAN   1*MiB

/*
===============================================================================================

 ShardMigrationWriter

===============================================================================================
*/

class ShardMigrationWriter
{
public:
    ShardMigrationWriter();
    ~ShardMigrationWriter();
    
    void                    Init(ShardServer* shardServer);
    void                    Reset();
    
    bool                    IsActive();
    uint64_t                GetShardID();
    uint64_t                GetQuorumID();
    uint64_t                GetNodeID();
    uint64_t                GetBytesSent();
    uint64_t                GetBytesTotal();
    uint64_t                GetThroughput();

    void                    Begin(ClusterMessage& request);
    void                    Abort();

    void                    OnResult();

private:
    void                    SendFirst();
    void                    SendNext();
    void                    SendCommit();
    void                    SendItem(StorageKeyValue* kv);
    void                    OnWriteReadyness();
    void                    OnBlockShard();
    void                    OnTimeout();

    bool                    isActive;
    bool                    sendFirst;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    uint64_t                bytesSent;
    uint64_t                bytesTotal;
    uint64_t                startTime;
    uint64_t                prevBytesSent;
    ShardServer*            shardServer;
    ShardQuorumProcessor*   quorumProcessor;
    StorageEnvironment*     environment;
    StorageBulkCursor*      cursor;
//    StorageAsyncBulkCursor* asyncCursor;
    StorageKeyValue*        kv;
    Countdown               onTimeout;
};

#endif
