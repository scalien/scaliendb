#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageShardProxy.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"

#define SHARD_DATABASE_YIELD_LIST_LENGTH        1000
#define SHARD_DATABASE_YIELD_TIME               CLOCK_RESOLUTION    // msec

class ShardServer; // forward

/*
===============================================================================================

 ShardDatabaseManager

===============================================================================================
*/

class ShardDatabaseManager
{
    typedef HashMap<uint64_t, StorageShardProxy*>   ShardMap;
    typedef InList<ClientRequest>                   ClientRequestList;

public:
    ShardDatabaseManager();

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
    void                    OnYieldStorageThreadsTimer();
    void                    OnExecuteReads();

    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageShardProxy       systemShard;
    ShardMap                quorumShards;
    Countdown               yieldStorageThreadsTimer;
    ClientRequestList       readRequests;
    YieldTimer              executeReads;
};

#endif
