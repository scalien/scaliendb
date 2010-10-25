#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "Framework/Storage/StorageEnvironment.h"
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
    typedef HashMap<uint64_t, StorageDatabase*>     DatabaseMap;
    typedef HashMap<uint64_t, StorageTable*>        TableMap;

public:
    void                    Init(ShardServer* shardServer);
    void                    Shutdown();
    
    StorageEnvironment*     GetEnvironment();
    StorageTable*           GetQuorumTable(uint64_t quorumID);
    StorageTable*           GetTable(uint64_t tableID);
    StorageShard*           GetShard(uint64_t shardID);

    void                    SetShards(List<uint64_t>& shards);
    
    void                    OnClientReadRequest(ClientRequest* request);
    void                    ExecuteWriteMessage(uint64_t paxosID,
                             uint64_t commandID, ShardMessage& message, ClientRequest* request);
    
private:
    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageDatabase*        systemDatabase;
    DatabaseMap             databases;
    TableMap                tables;
};

#endif
