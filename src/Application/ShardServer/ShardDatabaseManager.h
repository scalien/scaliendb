#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageShardProxy.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"

class ShardServer; // forward

/*
===============================================================================================

 ShardDatabaseManager

===============================================================================================
*/

class ShardDatabaseManager
{
    typedef HashMap<uint64_t, StorageShardProxy*>   ShardMap;

public:
    void                    Init(ShardServer* shardServer);
    void                    Shutdown();
    
    StorageEnvironment*     GetEnvironment();
    StorageShardProxy*      GetQuorumShard(uint64_t quorumID);

    void                    SetShards(SortedList<uint64_t>& shards);
    void                    SetQuorumShard(uint64_t quorumID);
    void                    RemoveDeletedDatabases();
    void                    RemoveDeletedTables();
    
    void                    OnClientReadRequest(ClientRequest* request);
    void                    ExecuteMessage(uint64_t paxosID,
                             uint64_t commandID, ShardMessage& message, ClientRequest* request);
    
private:
    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageShardProxy       systemShard;
    ShardMap                quorumShards;
};

#endif
