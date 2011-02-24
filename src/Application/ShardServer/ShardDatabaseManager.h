#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "System/Containers/InSortedList.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageShardProxy.h"
#include "Framework/Storage/StorageAsyncGet.h"
#include "Framework/Storage/StorageAsyncList.h"
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
 
 ShardDatabaseAsyncList -- helper class for async LIST operation
 
===============================================================================================
*/

class ShardDatabaseAsyncList : public StorageAsyncList
{
public:
    ClientRequest*          request;
    ShardDatabaseManager*   manager;
    ReadBuffer              shardLastKey;
    bool                    active;
    bool                    async;
    
    void                    OnShardComplete();
    void                    OnRequestComplete();
    void                    TryNextShard();
};

/*
===============================================================================================

 ShardDatabaseManager

===============================================================================================
*/

class ShardDatabaseManager
{
    typedef HashMap<uint64_t, StorageShardProxy*>   ShardMap;
    typedef InSortedList<ClientRequest>             ClientSortedRequestList;
    typedef InList<ClientRequest>                   ClientRequestList;

    friend class ShardDatabaseAsyncGet;
    friend class ShardDatabaseAsyncList;

public:
    ShardDatabaseManager();

    void                    Init(ShardServer* shardServer);
    void                    Shutdown();
    
    StorageEnvironment*     GetEnvironment();
    StorageShardProxy*      GetQuorumPaxosShard(uint64_t quorumID);
    StorageShardProxy*      GetQuorumLogShard(uint64_t quorumID);
    ConfigState*            GetConfigState();
    
    void                    DeleteQuorumPaxosShard(uint64_t quorumID);
    void                    DeleteQuorumLogShard(uint64_t quorumID);
    void                    DeleteDataShards(uint64_t quorumID);

    void                    SetShards(SortedList<uint64_t>& shards);
    void                    SetQuorumShards(uint64_t quorumID);
    void                    RemoveDeletedDataShards(SortedList<uint64_t>& myShardIDs);
    
    void                    OnClientReadRequest(ClientRequest* request);
    void                    OnClientListRequest(ClientRequest* request);
    void                    ExecuteMessage(uint64_t paxosID,
                             uint64_t commandID, ShardMessage& message, ClientRequest* request);
        
private:
    void                    OnYieldStorageThreadsTimer();
    void                    OnExecuteReads();
    void                    OnExecuteLists();

    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageShardProxy       systemShard;
    ShardMap                quorumPaxosShards;
    ShardMap                quorumLogShards;
    Countdown               yieldStorageThreadsTimer;
    ClientSortedRequestList readRequests;
    ClientRequestList       listRequests;
    YieldTimer              executeReads;
    ShardDatabaseAsyncGet   asyncGet;
    YieldTimer              executeLists;
    ShardDatabaseAsyncList  asyncList;
};

#endif
