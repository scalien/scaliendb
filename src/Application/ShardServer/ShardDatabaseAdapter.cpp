#include "ShardDatabaseAdapter.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "ShardServer.h"

static size_t Hash(uint64_t h)
{
    return h;
}

void ShardDatabaseAdapter::Init(ShardServer* shardServer_)
{
    shardServer = shardServer_;
    environment.InitCache(configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_CACHE_SIZE));
    environment.SetSync(configFile.GetBoolValue("database.sync", true));
    environment.Open(configFile.GetValue("database.dir", "db"));
    systemDatabase = environment.GetDatabase("system");
    REPLICATION_CONFIG->Init(systemDatabase->GetTable("system"));
}

void ShardDatabaseAdapter::Shutdown()
{
    DatabaseMap::Node*  dbNode;
    StorageDatabase*    database;

    for (dbNode = databases.First(); dbNode != NULL; dbNode = databases.Next(dbNode))
    {
        database = dbNode->Value();
        delete database;
    }

    environment.Close();
}

StorageEnvironment& ShardDatabaseAdapter::GetEnvironment()
{
    return environment;
}

StorageTable* ShardDatabaseAdapter::GetQuorumTable(uint64_t quorumID)
{
    Buffer name;
    
    name.Writef("%U", quorumID);
    name.NullTerminate();
    return systemDatabase->GetTable(name.GetBuffer());
}

StorageTable* ShardDatabaseAdapter::GetTable(uint64_t tableID)
{
    StorageTable* table;
    
    if (!tables.Get(tableID, table))
        return NULL;
    return table;
}

StorageShard* ShardDatabaseAdapter::GetShard(uint64_t shardID)
{
    ConfigShard*    shard;
    StorageTable*   table;
    
    shard = shardServer->GetConfigState().GetShard(shardID);
    if (!shard)
        return NULL;
    
    table = GetTable(shard->tableID);
    if (!table)
        return NULL;
    
    return table->GetShard(shardID);
}

void ShardDatabaseAdapter::SetShards(List<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;
    StorageDatabase*    database;
    StorageTable*       table;
    Buffer              name;
    ReadBuffer          firstKey;

    for (sit = shards.First(); sit != NULL; sit = shards.Next(sit))
    {
        shard = shardServer->GetConfigState().GetShard(*sit);
        assert(shard != NULL);
        
        if (!databases.Get(shard->databaseID, database))
        {
            name.Writef("%U", shard->databaseID);
            name.NullTerminate();

            database = environment.GetDatabase(name.GetBuffer());
            databases.Set(shard->databaseID, database);
        }
        
        if (!tables.Get(shard->tableID, table))
        {
            name.Writef("%U", shard->tableID);
            name.NullTerminate();
            
            table = database->GetTable(name.GetBuffer());
            assert(table != NULL);
            tables.Set(shard->tableID, table);
        }
        
        // check if key already exists
        firstKey.Wrap(shard->firstKey);
        if (!table->ShardExists(firstKey))
            table->CreateShard(*sit, firstKey);
    }
}
