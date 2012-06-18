#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Storage/StoragePageCache.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/CatchupMessage.h"
#include "Application/Common/ClientRequestCache.h"
#include "Application/ShardServer/ShardServerApp.h"

static inline bool LessThan(const uint64_t& a, const uint64_t& b)
{
    return a < b;
}

void ShardServer::Init(ShardServerApp* app, bool restoreMode, bool setNodeID, uint64_t myNodeID)
{
    unsigned        numControllers;
    uint64_t        nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;
    Buffer          buffer;

    shardServerApp = app;

    startTimestamp = Now();
    numRequests = 0;

    lockManager.Init();
    waitQueueManager.Init();
    databaseManager.Init(this); 
    heartbeatManager.Init(this);
    migrationWriter.Init(this);

    lockManager.SetLockExpireTime(configFile.GetIntValue("      lock.expireTime", LOCK_EXPIRE_TIME));
    lockManager.SetMaxCacheTime(configFile.GetIntValue("        lock.maxCacheTime", LOCK_CACHE_TIME));
    lockManager.SetMaxCacheCount(configFile.GetIntValue("       lock.maxCacheCount", LOCK_CACHE_COUNT));
    lockManager.SetMaxPoolCount(configFile.GetIntValue("        lock.maxPoolCount", LOCK_POOL_COUNT));

    waitQueueManager.SetWaitExpireTime(configFile.GetIntValue(" waitQueue.expireTime", WAITQUEUE_EXPIRE_TIME));
    waitQueueManager.SetMaxCacheTime(configFile.GetIntValue("   waitQueue.maxCacheTime", WAITQUEUE_CACHE_TIME));
    waitQueueManager.SetMaxCacheCount(configFile.GetIntValue("  waitQueue.maxCacheCount", WAITQUEUE_CACHE_COUNT));
    waitQueueManager.SetMaxPoolCount(configFile.GetIntValue("   waitQueue.maxPoolCount", WAITQUEUE_POOL_COUNT));

    REQUEST_CACHE->Init(configFile.GetIntValue("requestCache.size", 100*1000));

    if (restoreMode)
    {
        REPLICATION_CONFIG->SetClusterID(0);
        REPLICATION_CONFIG->SetNodeID(0);
        REPLICATION_CONFIG->SetRunID(0);
    }

    if (setNodeID)
    {
        REPLICATION_CONFIG->SetNodeID(myNodeID);
    }

    if (REPLICATION_CONFIG->GetNodeID() > 0)
        Log_Message("My nodeID is %U", REPLICATION_CONFIG->GetNodeID());

    runID = REPLICATION_CONFIG->GetRunID();
    runID += 1;
    REPLICATION_CONFIG->SetRunID(runID);
    REPLICATION_CONFIG->Commit();
    Log_Trace("rundID: %U", runID);

    if (MY_NODEID > 0)
        CONTEXT_TRANSPORT->SetSelfNodeID(MY_NODEID);
    
    CONTEXT_TRANSPORT->SetClusterID(REPLICATION_CONFIG->GetClusterID());
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", (int) nodeID, "");
        endpoint.Set(str, true);
        CONTEXT_TRANSPORT->AddConnection(nodeID, endpoint);
        configServers.Append(nodeID);
        // this will cause the node to connect to the controllers
        // and if my nodeID is not set the MASTER will automatically send
        // me a SetNodeID cluster message
    }

    CONTEXT_TRANSPORT->SetClusterContext(this);
}

void ShardServer::Shutdown()
{
    if (migrationWriter.IsActive())
        migrationWriter.Abort();

    quorumProcessors.DeleteList();
    databaseManager.Shutdown();
    waitQueueManager.Shutdown();
    lockManager.Shutdown();
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    REQUEST_CACHE->Shutdown();
}

ShardQuorumProcessor* ShardServer::GetQuorumProcessor(uint64_t quorumID)
{
    ShardQuorumProcessor* it;
    
    FOREACH (it, quorumProcessors)
    {
        if (it->GetQuorumID() == quorumID)
            return it;
    }
    
    return NULL;
}

ShardServer::QuorumProcessorList* ShardServer::GetQuorumProcessors()
{
    return &quorumProcessors;
}

ShardDatabaseManager* ShardServer::GetDatabaseManager()
{
    return &databaseManager;
}

ShardLockManager* ShardServer::GetLockManager()
{
    return &lockManager;
}

ShardWaitQueueManager* ShardServer::GetWaitQueueManager()
{
    return &waitQueueManager;
}

ShardMigrationWriter* ShardServer::GetShardMigrationWriter()
{
    return &migrationWriter;
}

ShardHeartbeatManager* ShardServer::GetHeartbeatManager()
{
    return &heartbeatManager;
}

ConfigState* ShardServer::GetConfigState()
{
    return &configState;
}

ShardServerApp* ShardServer::GetShardServerApp()
{
    return shardServerApp;
}

void ShardServer::BroadcastToControllers(Message& message)
{
    uint64_t* itNodeID;

    FOREACH (itNodeID, configServers)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, message);
}

bool ShardServer::IsValidClientRequest(ClientRequest* request)
{
     return request->IsShardServerRequest();
}

void ShardServer::OnClientRequest(ClientRequest* request)
{
    ConfigShard*            shard;
    ShardQuorumProcessor*   quorumProcessor;
    
    numRequests += 1;

    if (request->IsTransaction())
    {
        quorumProcessor = GetQuorumProcessor(request->quorumID);
        if (!quorumProcessor)
        {
            request->response.NoService();
            request->OnComplete();
            return;
        }
        quorumProcessor->OnClientRequest(request);
        return;
    }
    
    shard = configState.GetShard(request->tableID, ReadBuffer(request->key));
    if (!shard)
    {
        Log_Trace();
        request->response.BadSchema();
        request->OnComplete();
        return;
    }
    
    quorumProcessor = GetQuorumProcessor(shard->quorumID);
    if (!quorumProcessor)
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
        
    request->shardID = shard->shardID;    
    quorumProcessor->OnClientRequest(request);
}

void ShardServer::OnClientClose(ClientSession* session)
{
    ClientRequest* request;
    
    if (!session->IsTransactional())
        return;
    
    if (session->IsCommitting())
        return;

    FOREACH_POP(request, session->transaction)
    {
        ASSERT(request->type != CLIENTREQUEST_UNDEFINED);
        request->response.NoResponse();
        request->OnComplete();
    }

    lockManager.Unlock(session->lockKey);
    Log_Debug("Lock %B released due to disconnect.", &session->lockKey);
    
    session->Init();
}

void ShardServer::OnClusterMessage(uint64_t nodeID, ClusterMessage& message)
{
    ShardQuorumProcessor*   quorumProcessor;
    ConfigShard*            configShard;
    
    Log_Trace();
        
    switch (message.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            if (!CONTEXT_TRANSPORT->IsAwaitingNodeID())
                return;
            CONTEXT_TRANSPORT->SetSelfNodeID(message.nodeID);
            REPLICATION_CONFIG->SetNodeID(message.nodeID);
            ASSERT(REPLICATION_CONFIG->GetClusterID() == 0);
            CONTEXT_TRANSPORT->SetClusterID(message.clusterID);
            REPLICATION_CONFIG->SetClusterID(message.clusterID);
            REPLICATION_CONFIG->Commit();
            Log_Trace("My nodeID is %U", message.nodeID);
            Log_Message("NodeID set to %U", message.nodeID);
            break;
        case CLUSTERMESSAGE_UNREGISTER_STOP:
            Log_Message();
            Log_Message("*****************************************************************");
            Log_Message("*                                                               *");
            Log_Message("*  This shard server has been unregistered on the controllers!  *");
            Log_Message("*  ScalienDB will now exit...                                   *");
            Log_Message("*                                                               *");
            Log_Message("*****************************************************************");
            Log_Message();
            STOP_FAIL(0, "Aborting due to shard server unregistration...");
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            OnSetConfigState(nodeID, message);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            quorumProcessor = GetQuorumProcessor(message.quorumID);
            if (quorumProcessor)
                quorumProcessor->OnReceiveLease(nodeID, message);
            break;
        /* shard migration */
        case CLUSTERMESSAGE_SHARDMIGRATION_INITIATE:
            configShard = configState.GetShard(message.srcShardID);
            ASSERT(configShard != NULL);
            quorumProcessor = GetQuorumProcessor(configShard->quorumID);
            ASSERT(quorumProcessor != NULL);
            if (!quorumProcessor->IsPrimary())
            {
                if (migrationWriter.IsActive())
                    migrationWriter.Abort();
                break;
            }
            
            migrationWriter.Begin(message);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_PAUSE:
            if (migrationWriter.IsActive())
                migrationWriter.Pause();
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_RESUME:
            if (migrationWriter.IsActive())
                migrationWriter.Resume();
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
        case CLUSTERMESSAGE_SHARDMIGRATION_COMMIT:
            quorumProcessor = GetQuorumProcessor(message.quorumID);
            ASSERT(quorumProcessor != NULL);
            if (!quorumProcessor->IsPrimary())
            {
                if (migrationWriter.IsActive())
                    migrationWriter.Abort();
                break;
            }
            quorumProcessor->OnShardMigrationClusterMessage(nodeID, message);
            break;
        
        case CLUSTERMESSAGE_HELLO:
            break;

        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
    // nothing
}

void ShardServer::OnConnectionEnd(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
    // nothing
}

bool ShardServer::OnAwaitingNodeID(Endpoint /*endpoint*/)
{
    // always drop
    return true;
}

bool ShardServer::IsLeaseKnown(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;
    ConfigQuorum*           configQuorum;
    
    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return false;
    
    if (quorumProcessor->IsPrimary())
        return true;
    
    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return false;
    
    if (!configQuorum->hasPrimary)
        return false;
    
    // if the quorumProcessor says we're not the primary
    // then we're not the primary, even if the config state says so
    if (configQuorum->primaryID == MY_NODEID)
        return false;
    
    return true;
}

bool ShardServer::IsLeaseOwner(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;

    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return false;

    return quorumProcessor->IsPrimary();
}

uint64_t ShardServer::GetLeaseOwner(uint64_t quorumID)
{
    ShardQuorumProcessor*   quorumProcessor;
    ConfigQuorum*           configQuorum;
    
    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
        return UNDEFINED_NODEID;
    
    if (quorumProcessor->IsPrimary())
        return MY_NODEID;

    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return UNDEFINED_NODEID;

    if (!configQuorum->hasPrimary)
        return UNDEFINED_NODEID;

    // if the quorumProcessor says we're not the primary
    // then we're not the primary, even if the config state says so
    if (configQuorum->primaryID == MY_NODEID)
        return UNDEFINED_NODEID;

    return configQuorum->primaryID;
}

unsigned ShardServer::GetHTTPPort()
{
    return configFile.GetIntValue("http.port", 8080);
}

unsigned ShardServer::GetSDBPPort()
{
    return configFile.GetIntValue("sdbp.port", 7080);
}

void ShardServer::GetMemoryUsageBuffer(Buffer& buffer)
{
    uint64_t                shardMemoryUsage;
    uint64_t                logSegmentMemoryUsage;
    uint64_t                totalMemory;
    uint64_t                quorumMessageCacheSize;
    uint64_t                quorumMessageListSize;
    uint64_t                quorumAppendStateSize;
    uint64_t                quorumContextSize;
    uint64_t                connectionMemoryUsage;
    ShardQuorumProcessor*   quorumProcessor;
    char                    humanBuf[5];

    buffer.Clear();
    totalMemory = 0;

    shardMemoryUsage = GetDatabaseManager()->GetEnvironment()->GetShardMemoryUsage();
    buffer.Appendf("Shard memory usage: %s\n", HumanBytes(shardMemoryUsage, humanBuf));
    totalMemory += shardMemoryUsage;

    logSegmentMemoryUsage = GetDatabaseManager()->GetEnvironment()->GetLogSegmentMemoryUsage();
    buffer.Appendf("Log segment memory usage: %s\n", HumanBytes(logSegmentMemoryUsage, humanBuf));
    totalMemory += logSegmentMemoryUsage;

    buffer.Appendf("Storage cache usage: %s\n", HumanBytes(StoragePageCache::GetSize(), humanBuf));
    totalMemory += StoragePageCache::GetSize();

    buffer.Appendf("Storage list page cache usage: %s\n", HumanBytes(StorageDataPageCache::GetCacheSize(), humanBuf));
    totalMemory += StorageDataPageCache::GetCacheSize();

    buffer.Appendf("Client request cache usage: %s\n", HumanBytes(REQUEST_CACHE->GetMemorySize(), humanBuf));
    totalMemory += REQUEST_CACHE->GetMemorySize();

    quorumMessageCacheSize = 0;
    quorumMessageListSize = 0;
    quorumAppendStateSize = 0;
    quorumContextSize = 0;
    FOREACH (quorumProcessor, quorumProcessors)
    {
        quorumMessageCacheSize += quorumProcessor->GetMessageCacheSize();
        quorumMessageListSize += quorumProcessor->GetMessageListSize();
        quorumAppendStateSize += quorumProcessor->GetShardAppendStateSize();
        quorumContextSize += quorumProcessor->GetQuorumContextSize();
    }
    buffer.Appendf("Message cache usage: %s\n", HumanBytes(quorumMessageCacheSize, humanBuf));
    totalMemory += quorumMessageCacheSize;

    buffer.Appendf("Message list usage: %s\n", HumanBytes(quorumMessageListSize, humanBuf));
    totalMemory += quorumMessageListSize;

    buffer.Appendf("Shard append state usage: %s\n", HumanBytes(quorumAppendStateSize, humanBuf));
    totalMemory += quorumAppendStateSize;

    buffer.Appendf("Shard quorum context usage: %s\n", HumanBytes(quorumContextSize, humanBuf));
    totalMemory += quorumContextSize;

    connectionMemoryUsage = GetShardServerApp()->GetMemoryUsage();
    buffer.Appendf("Connection memory usage: %s\n", HumanBytes(connectionMemoryUsage, humanBuf));
    totalMemory += connectionMemoryUsage;

    buffer.Appendf("Total memory usage: %s\n", HumanBytes(totalMemory, humanBuf));
    buffer.Appendf("Total memory usage reported by system: %s\n", HumanBytes(GetProcessMemoryUsage(), humanBuf));
}

unsigned ShardServer::GetNumSDBPClients()
{
    return shardServerApp->GetNumSDBPClients();
}

uint64_t ShardServer::GetStartTimestamp()
{
    return startTimestamp;
}

uint64_t ShardServer::GetNumRequests()
{
    return numRequests;
}

void ShardServer::OnSetConfigState(uint64_t nodeID, ClusterMessage& message)
{
    ConfigQuorum*           configQuorum;
    ShardQuorumProcessor*   quorumProcessor;
    
    if (!message.configState.hasMaster)
        return;

    if (message.configState.masterID != nodeID)
        return;

    configState = message.configState;

    ResetChangedConnections();

    TryDeleteQuorumProcessors();

    ConfigureQuorums();

    FOREACH(quorumProcessor, quorumProcessors)
    {
        configQuorum = configState.GetQuorum(quorumProcessor->GetQuorumID());
        if (!configQuorum || !configQuorum->IsMember(MY_NODEID))
            continue; // in case it was not deleted in TryDeleteQuorumProcessors() above

        quorumProcessor->OnSetConfigState();     
    }

    TryDeleteShards();

    Log_Trace("Got new configState, master is %d", 
     configState.hasMaster ? (int) configState.masterID : -1);
}

void ShardServer::ResetChangedConnections()
{
    ConfigShardServer* configShardServer;

    FOREACH(configShardServer, configState.shardServers)
    {
        if (CONTEXT_TRANSPORT->HasConnection(configShardServer->nodeID))
        {
            if (CONTEXT_TRANSPORT->GetEndpoint(configShardServer->nodeID) != configShardServer->endpoint)
            {
                CONTEXT_TRANSPORT->DropConnection(configShardServer->nodeID);
                CONTEXT_TRANSPORT->AddConnection(configShardServer->nodeID, configShardServer->endpoint);
            }
        }
    }
}

void ShardServer::TryDeleteQuorumProcessors()
{
    ShardQuorumProcessor*   quorumProcessor;
    ShardQuorumProcessor*   next;
    ConfigQuorum*           configQuorum;
    
    // delete quorum processors no longer required:
    //   - the quorum was deleted on the controllers
    //   - this node is no longer a member of the quorum
    for (quorumProcessor = quorumProcessors.First(); quorumProcessor != NULL; quorumProcessor = next)
    {
        next = quorumProcessors.Next(quorumProcessor); // it may be removed at the end of the block
        configQuorum = configState.GetQuorum(quorumProcessor->GetQuorumID());
        if (!configQuorum || !configQuorum->IsMember(MY_NODEID))
            TryDeleteQuorumProcessor(quorumProcessor);
    }
}

void ShardServer::TryDeleteQuorumProcessor(ShardQuorumProcessor* quorumProcessor)
{
    ConfigShard* configShard;

    // if the PaxosAcceptor inside the QuorumProcessor is doing an async commit,
    // then we can't delete the quorumProcessor, because the OnComplete() would crash
    if (databaseManager.GetEnvironment()->IsCommitting(quorumProcessor->GetQuorumID()))
        return;

    if (migrationWriter.IsActive())
    {
        configShard = configState.GetShard(migrationWriter.GetShardID());
        if (quorumProcessor->GetQuorumID() == configShard->quorumID)
            migrationWriter.Abort();
    }

    databaseManager.DeleteQuorum(quorumProcessor->GetQuorumID());

    quorumProcessor->Shutdown();
    quorumProcessors.Remove(quorumProcessor);
    delete quorumProcessor;
}

void ShardServer::ConfigureQuorums()
{
    ConfigQuorum* configQuorum;

    FOREACH (configQuorum, configState.quorums)
    {
        if (!configQuorum->IsMember(MY_NODEID))
            continue;

        ConfigureQuorum(configQuorum); // also creates quorum
    }
}

void ShardServer::ConfigureQuorum(ConfigQuorum* configQuorum)
{
    uint64_t*               itNodeID;
    ConfigShardServer*      shardServer;
    ShardQuorumProcessor*   quorumProcessor;
    
    Log_Trace();
    
    quorumProcessor = GetQuorumProcessor(configQuorum->quorumID);
    if (!quorumProcessor)
    {
        databaseManager.SetQuorumShards(configQuorum->quorumID);
        
        quorumProcessor = new ShardQuorumProcessor;
        quorumProcessor->Init(configQuorum, this);
        quorumProcessor->SetReplicationLimit(configFile.GetIntValue("replicationLimit", 0));
        quorumProcessors.Append(quorumProcessor);
    }

    // add nodes to CONTEXT_TRANSPORT
    FOREACH (itNodeID, configQuorum->activeNodes)
    {
        shardServer = configState.GetShardServer(*itNodeID);
        ASSERT(shardServer != NULL);
        CONTEXT_TRANSPORT->AddConnection(*itNodeID, shardServer->endpoint);
    }

    databaseManager.SetShards(configQuorum->shards);
}

void ShardServer::TryDeleteShards()
{
    uint64_t*               itShardID;
    ConfigQuorum*           configQuorum;
    SortedList<uint64_t>    shardIDs;
    
    FOREACH (configQuorum, configState.quorums)
    {
        if (!configQuorum->IsMember(MY_NODEID))
            continue;

        FOREACH (itShardID, configQuorum->shards)
            shardIDs.Add(*itShardID);
    }
    
    if (configState.isMigrating)
    {
        shardIDs.Add(configState.migrateSrcShardID);
        shardIDs.Add(configState.migrateDstShardID);
    }
    
    databaseManager.RemoveDeletedDataShards(shardIDs);
}
