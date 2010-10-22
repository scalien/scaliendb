#ifndef SHARDDATABASEADAPTER_H
#define SHARDDATABASEADAPTER_H

#include "System/Containers/HashMap.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Application/ConfigState/ConfigState.h"

class ShardServer; // forward

/*
===============================================================================================

 ShardDatabaseAdapter

===============================================================================================
*/

class ShardDatabaseAdapter
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
    
private:
    ShardServer*            shardServer;
    StorageEnvironment      environment;
    StorageDatabase*        systemDatabase;
    DatabaseMap             databases;
    TableMap                tables;
};

#endif
