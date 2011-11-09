#include "ConfigDatabaseManager.h"
#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "Framework/Storage/StoragePageCache.h"
#include "System/Config.h"

void ConfigDatabaseManager::Init()
{
    unsigned            num;
    uint64_t            nodeID;
    Buffer              envpath;
    ReadBuffer          rb;
    ConfigController*   controller;
    
    envpath.Writef("%s", configFile.GetValue("database.dir", "db"));
    environment.Open(envpath);
    environment.config.numLogSegmentFileChunks = 0;

    environment.CreateShard(1, QUORUM_DATABASE_SYSTEM_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_STANDARD);
    environment.CreateShard(1, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_DUMP);
    environment.CreateShard(1, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_LOG);

    systemConfigShard.Init(&environment, QUORUM_DATABASE_SYSTEM_CONTEXT, 1);
    quorumPaxosShard.Init(&environment, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, 1);
    quorumLogShard.Init(&environment, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, 1);
    
    paxosID = 0;
    configState.Init();
    Read();
    
    num = configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < num; nodeID++)
    {
        controller = new ConfigController;
        controller->nodeID = nodeID;
        rb = configFile.GetListValue("controllers", nodeID, "");
        controller->endpoint.Set(rb, true);
        configState.controllers.Append(controller);
    }

}

void ConfigDatabaseManager::Shutdown()
{
    environment.Close();
    StoragePageCache::Shutdown();
}

ConfigState* ConfigDatabaseManager::GetConfigState()
{
    return &configState;
}

void ConfigDatabaseManager::SetPaxosID(uint64_t paxosID_)
{
    paxosID = paxosID_;
}

uint64_t ConfigDatabaseManager::GetPaxosID()
{
    return paxosID;
}

StorageShardProxy* ConfigDatabaseManager::GetSystemShard()
{
    return &systemConfigShard;
}

StorageShardProxy* ConfigDatabaseManager::GetQuorumPaxosShard()
{
    return &quorumPaxosShard;
}

StorageShardProxy* ConfigDatabaseManager::GetQuorumLogShard()
{
    return &quorumLogShard;
}

bool ConfigDatabaseManager::ShardExists(uint64_t tableID, ReadBuffer firstKey)
{
    ConfigShard* shard;
    
    FOREACH (shard, configState.shards)
    {
        if (shard->tableID == tableID && ReadBuffer::Cmp(firstKey, shard->firstKey) == 0)
            return true;
    }
    
    return false;
}

void ConfigDatabaseManager::Read()
{
    bool                ret;
    ReadBuffer          value;
    int                 read;
    
    ret =  true;
    
    if (!systemConfigShard.Get(ReadBuffer("state"), value))
    {
        Log_Message("Starting with empty database...");
        return;
    }
    
    read = value.Readf("%U:", &paxosID);
    if (read < 2)
        ASSERT_FAIL();
    
    value.Advance(read);
    
    if (!configState.Read(value))
        ASSERT_FAIL();

    Log_Trace("%R", &value);
}

void ConfigDatabaseManager::Write()
{
    writeBuffer.Writef("%U:", paxosID);
    configState.Write(writeBuffer);
    systemConfigShard.Set(ReadBuffer("state"), ReadBuffer(writeBuffer));
}
