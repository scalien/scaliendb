#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "System/Containers/InSortedList.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageShardProxy.h"
#include "Framework/Storage/StorageAsyncGet.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClientRequest.h"
#include "ShardMessage.h"

#define SHARD_DATABASE_YIELD_LIST_LENGTH        1000
#define SHARD_DATABASE_YIELD_THREADS_TIMEOUT    1000

class ShardServer;              // forward
class ShardDatabaseManager;     // forward
/*
===============================================================================================
 
 ShardDatabaseAsyncGet -- helper class for async GET operation
 
===============================================================================================
*/

class ShardDatabaseAsyncGet : public StorageAsyncGet
{
public:
    ClientRequest*          request;
    ShardDatabaseManager*   manager;
    bool                    active;
    bool                    async;
    
    void                    OnRequestComplete();
};

/*
===============================================================================================

 ShardDatabaseManager

===============================================================================================
*/

class ShardDatabaseManager
{
    typedef HashMap<uint64_t, StorageShardProxy*>   ShardMap;
//    typedef InList<ClientRequest>                   ClientRequestList;
    typedef InSortedList<ClientRequest>             ClientRequestList;

    friend class ShardDatabaseAsyncGet;

public:
    ShardDatabaseManager();

    void                    Init(ShardServer* shardServer);
    void                    Shutdown();
    
    StorageEnvironment*     GetEnvironment();
    StorageShardProxy*      GetQuorumPaxosShard(uint64_t quorumID);
    StorageShardProxy*      GetQuorumLogShard(uint64_t quorumID);

    void                    SetShards(SortedList<uint64_t>& shards);
    void                    SetQuorumShards(uint64_t quorumID);
    void                    RemoveDeletedDatabases();
    void                    RemoveDeletedTables();
    
    void                    OnClientReadRequest(ClientRequest* request);
    void                    ExecuteMessage(uint64_t paxosID,
                             uint64_t commandID, ShardMessage& message, ClientRequest* request);
    
    void                    OnAsyncReadComplete();
    
private:
    void                    OnYieldStorageThreadsTimer();
    void                    OnExecuteReads();

    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageShardProxy       systemShard;
    ShardMap                quorumPaxosShards;
    ShardMap                quorumLogShards;
    Countdown               yieldStorageThreadsTimer;
    ClientRequestList       readRequests;
    YieldTimer              executeReads;
    ShardDatabaseAsyncGet   asyncGet;
};

#endif
