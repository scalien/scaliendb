#ifndef SHARDMIGRATIONWRITER_H
#define SHARDMIGRATIONWRITER_H

#include "Framework/Storage/StorageBulkCursor.h"
#include "Application/Common/ClusterMessage.h"

class ShardServer;

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
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    StorageEnvironment*     environment;
    StorageBulkCursor*      cursor;
    StorageKeyValue*        kv;
};

#endif
