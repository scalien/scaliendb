#include "ConfigDatabaseManager.h"
#include "System/FileSystem.h"
#include "Framework/Storage/FDGuard.h"
#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "Framework/Storage/StoragePageCache.h"
#include "Framework/Storage/StorageConfig.h"
#include "Framework/Storage/StorageDataPage.h"
#include "Application/Common/ContextTransport.h"
#include "System/Config.h"

void ConfigDatabaseManager::Init(bool restoreMode)
{
    Buffer          envpath;
    StorageConfig   sc;
    
    sc.SetChunkSize(            (uint64_t) configFile.GetInt64Value("database.chunkSize",				64*MiB  ));
    sc.SetLogSegmentSize(       (uint64_t) configFile.GetInt64Value("database.logSegmentSize",			64*MiB  ));
    sc.SetFileChunkCacheSize(   (uint64_t) configFile.GetInt64Value("database.fileChunkCacheSize",		256*MiB ));
    sc.SetMemoChunkCacheSize(   (uint64_t) configFile.GetInt64Value("database.memoChunkCacheSize",		1*GiB   ));
    sc.SetLogSize(              (uint64_t) configFile.GetInt64Value("database.logSize",					64*MiB  ));
    sc.SetMergeBufferSize(      (uint64_t) configFile.GetInt64Value("database.mergeBufferSize",			10*MiB  ));
    sc.SetMergeYieldFactor(     (uint64_t) configFile.GetInt64Value("database.mergeYieldFactor",        100     ));
    sc.SetSyncGranularity(      (uint64_t) configFile.GetInt64Value("database.syncGranularity",			16*MiB  ));
    sc.SetWriteGranularity(     (uint64_t) configFile.GetInt64Value("database.writeGranularity",		STORAGE_WRITE_GRANULARITY));
    sc.SetReplicatedLogSize(    (uint64_t) configFile.GetInt64Value("database.replicatedLogSize",		0       ));
    sc.SetAbortWaitingListsNum( (uint64_t) configFile.GetInt64Value("database.abortWaitingListsNum",	0       ));
    sc.SetListDataPageCacheSize((uint64_t) configFile.GetInt64Value("database.listDataPageCacheSize",   1*MB    ));

    envpath.Writef("%s", configFile.GetValue("database.dir", "db"));
    environment.Open(envpath, sc);

    if (configFile.GetBoolValue("database.merge", true))
        environment.SetMergeEnabled(true);
    environment.SetMergeCpuThreshold(configFile.GetIntValue("database.mergeCpuThreshold", STORAGE_DEFAULT_MERGE_CPU_THRESHOLD));

    environment.CreateShard(1, QUORUM_DATABASE_SYSTEM_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_STANDARD);
    environment.CreateShard(1, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_DUMP);
    environment.CreateShard(1, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, 1, 0, "", "", true, STORAGE_SHARD_TYPE_LOG);

    systemConfigShard.Init(&environment, QUORUM_DATABASE_SYSTEM_CONTEXT, 1);
    quorumPaxosShard.Init(&environment, QUORUM_DATABASE_QUORUM_PAXOS_CONTEXT, 1);
    quorumLogShard.Init(&environment, QUORUM_DATABASE_QUORUM_LOG_CONTEXT, 1);
    
    paxosID = 0;
    configState.Init();
    Read();

    if (restoreMode)
    {
        Log_Message("Attempting to read config state from file 'configState' in the db folder...");
        if (ReadConfigStateFromFile())
            Log_Message("Success.");
        else
            STOP_FAIL(1, "Failed to read config state from file 'configState' in the db folder...");
        Write();
    }
    
    SetControllers();
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

StorageEnvironment* ConfigDatabaseManager::GetEnvironment()
{
    return &environment;
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

void ConfigDatabaseManager::SetControllers()
{
    unsigned            num;
    uint64_t            nodeID;
    ReadBuffer          rb;
    ConfigController*   controller;

    configState.controllers.DeleteList();

    num = configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < num; nodeID++)
    {
        controller = new ConfigController;
        controller->nodeID = nodeID;
        rb = configFile.GetListValue("controllers", nodeID, "");
        controller->endpoint.Set(rb, true);
        controller->isConnected = CONTEXT_TRANSPORT->IsConnected(controller->nodeID);
        configState.controllers.Append(controller);
    }
}

bool ConfigDatabaseManager::ReadConfigStateFromFile()
{
    FDGuard     fd;
    Buffer      configStateFile;
    Buffer      configStateBuffer;
    ReadBuffer  rb;
    int64_t     size;

    configStateFile.Write(environment.envPath);
    configStateFile.Append("configState");
    configStateFile.NullTerminate();

    if (fd.Open(configStateFile.GetBuffer(), FS_READONLY) == INVALID_FD)
    {
        Log_Message("Unable to open file 'configState'");
        return false;	
    }
    size = FS_FileSize(fd.GetFD());
    if (size <= 0)
    {
        Log_Message("File 'configState' is corrupt or of zero length");
        return false;
    }
    configStateBuffer.Allocate(size);
    if (FS_FileRead(fd.GetFD(), configStateBuffer.GetBuffer(), size) != (ssize_t) size)
    {
        Log_Message("Unable to read from file 'configState'");
        return false;
    }
    configStateBuffer.SetLength(size);
    rb.Wrap(configStateBuffer);

    if (!configState.Read(rb, false))
    {
        Log_Message("Error parsing the contents of the config state");
        return false;
    }
    return true;
}
