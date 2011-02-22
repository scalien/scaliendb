#ifndef SHARDMIGRATIONWRITER_H
#define SHARDMIGRATIONWRITER_H

#include "Framework/Storage/StorageBulkCursor.h"
#include "Application/Common/ClusterMessage.h"

class ShardServer;
class ShardQuorumProcessor;

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

    void                    Begin(ClusterMessage& request);
    void                    Abort();

private:
    void                    SendFirst();
    void                    SendNext();
    void                    SendCommit();
    void                    SendItem(StorageKeyValue* kv);
    void                    OnWriteReadyness();

    bool                    isActive;
    bool                    sendFirst;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    ShardServer*            shardServer;
    ShardQuorumProcessor*   quorumProcessor;
    StorageEnvironment*     environment;
    StorageBulkCursor*      cursor;
    StorageKeyValue*        kv;
};

#endif
