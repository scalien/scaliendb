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

void ShardServer::Init(ShardServerApp* app)
{
    unsigned        numControllers;
    uint64_t        nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;
    Buffer          buffer;

    shardServerApp = app;

    databaseManager.Init(this); 
    heartbeatManager.Init(this);
    migrationWriter.Init(this);
    REQUEST_CACHE->Init(configFile.GetIntValue("requestCache.size", 100*1000));

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
    quorumProcessors.DeleteList();
    databaseManager.Shutdown();
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

ShardMigrationWriter* ShardServer::GetShardMigrationWriter()
{
    return &migrationWriter;
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
    
    if (request->type == CLIENTREQUEST_SUBMIT)
    {
        quorumProcessor = GetQuorumProcessor(request->quorumID);
        if (!quorumProcessor)
        {
            request->response.NoResponse();
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
    
    if (quorumProcessor->GetBlockedShardID() == shard->shardID)
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    request->shardID = shard->shardID;    
    quorumProcessor->OnClientRequest(request);
}

void ShardServer::OnClientClose(ClientSession* /*session*/)
{
    // nothing
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
            STOP_FAIL(0, "Abording due to shard server unregistration...");
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            OnSetConfigState(message);
            Log_Trace("Got new configState, master is %d", 
             configState.hasMaster ? (int) configState.masterID : -1);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            quorumProcessor = GetQuorumProcessor(message.quorumID);
            if (quorumProcessor)
                quorumProcessor->OnReceiveLease(message);
            Log_Trace("Recieved lease, quorumID = %U, proposalID =  %U",
             message.quorumID, message.proposalID);
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
    
    // we already checked this case
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
        return false;
    
    if (quorumProcessor->IsPrimary())
        return MY_NODEID;

    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return 0;   
    
    if (!configQuorum->hasPrimary)
        return 0;
    
    // we already checked this case
    if (configQuorum->primaryID == MY_NODEID)
        return 0;
    
    return configQuorum->primaryID;
}

void ShardServer::OnSetConfigState(ClusterMessage& message)
{
    uint64_t*               itShardID;
    ReadBuffer              splitKey;
    ConfigShard*            configShard;
    ConfigQuorum*           configQuorum;
    ConfigShardServer*      configShardServer;
    ShardQuorumProcessor*   quorumProcessor;
    ShardQuorumProcessor*   next;
    SortedList<uint64_t>    myShardIDs;
    
//    Log_Debug("ShardServer::OnSetConfigState");

    configState = message.configState;
    configShardServer = configState.GetShardServer(MY_NODEID);

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
    
    // look for removal
    for (quorumProcessor = quorumProcessors.First(); quorumProcessor != NULL; quorumProcessor = next)
    {
        configQuorum = configState.GetQuorum(quorumProcessor->GetQuorumID());
        if (configQuorum == NULL)
            goto DeleteQuorum;
        
        next = quorumProcessors.Next(quorumProcessor);
        
        if (configQuorum->IsActiveMember(MY_NODEID))
        {                
            if (configQuorum->isActivatingNode || configQuorum->activatingNodeID == MY_NODEID)
            {
                quorumProcessor->OnActivation();
                heartbeatManager.OnActivation();
            }

            quorumProcessor->RegisterPaxosID(configQuorum->paxosID);

            if (quorumProcessor->IsPrimary())
            {
                // look for shard splits
                FOREACH (itShardID, configQuorum->shards)
                {
                    configShard = configState.GetShard(*itShardID);
                    if (databaseManager.GetEnvironment()->ShardExists(QUORUM_DATABASE_DATA_CONTEXT, *itShardID))
                        continue;
                    if (configShard->state == CONFIG_SHARD_STATE_SPLIT_CREATING)
                    {
                        Log_Trace("Splitting shard (parent shardID = %U, new shardID = %U)...",
                         configShard->parentShardID, configShard->shardID);
                        splitKey.Wrap(configShard->firstKey);
                        quorumProcessor->TrySplitShard(configShard->parentShardID,
                         configShard->shardID, splitKey);
                    }
                    else if (configShard->state == CONFIG_SHARD_STATE_TRUNC_CREATING)
                    {
                        quorumProcessor->TryTruncateTable(configShard->tableID, configShard->shardID);
                    }
                }
            }
            continue;
        }
        if (configQuorum->IsInactiveMember(MY_NODEID))
        {
            if (quorumProcessor->IsCatchupActive())
                quorumProcessor->AbortCatchup();
            quorumProcessor->RegisterPaxosID(configQuorum->paxosID);
            continue;
        }
        
DeleteQuorum:
        // if the PaxosAcceptor inside the QuorumProcessor is doing an async commit,
        // then we can't delete the quorumProcessor, because the OnComplete() would crash
        if (databaseManager.GetEnvironment()->IsCommiting(quorumProcessor->GetQuorumID()))
        {
            next = quorumProcessors.Next(quorumProcessor);
            continue;
        }

        databaseManager.DeleteQuorumPaxosShard(quorumProcessor->GetQuorumID());
        databaseManager.DeleteQuorumLogShard(quorumProcessor->GetQuorumID());
        databaseManager.DeleteDataShards(quorumProcessor->GetQuorumID());

        next = quorumProcessors.Remove(quorumProcessor);
        quorumProcessor->Shutdown();
        delete quorumProcessor;
    }

    // check changes in active or inactive node list
    FOREACH (configQuorum, configState.quorums)
    {
        if (configQuorum->IsActiveMember(MY_NODEID))
        {
            ConfigureQuorum(configQuorum); // also creates quorum
            FOREACH (itShardID, configQuorum->shards)
                myShardIDs.Add(*itShardID);
        }
        if (configQuorum->IsInactiveMember(MY_NODEID))
        {
            ConfigureQuorum(configQuorum); // also creates quorum
            quorumProcessor = GetQuorumProcessor(configQuorum->quorumID);
            ASSERT(quorumProcessor != NULL);
            quorumProcessor->TryReplicationCatchup();
            FOREACH (itShardID, configQuorum->shards)
                myShardIDs.Add(*itShardID);
        }
    }
    
    if (configState.isMigrating)
    {
        myShardIDs.Add(configState.migrateSrcShardID);
        myShardIDs.Add(configState.migrateDstShardID);
    }
    
    databaseManager.RemoveDeletedDataShards(myShardIDs);
}

void ShardServer::ConfigureQuorum(ConfigQuorum* configQuorum)
{
    uint64_t                quorumID;
    uint64_t*               itNodeID;
    ConfigShardServer*      shardServer;
    ShardQuorumProcessor*   quorumProcessor;
    SortedList<uint64_t>    activeNodes;
    
    Log_Trace();
    
    quorumID = configQuorum->quorumID;
    quorumProcessor = GetQuorumProcessor(quorumID);
    if (quorumProcessor == NULL)
    {
        databaseManager.SetQuorumShards(quorumID);
        
        quorumProcessor = new ShardQuorumProcessor;
        quorumProcessor->Init(configQuorum, this);

        quorumProcessors.Append(quorumProcessor);
        FOREACH (itNodeID, configQuorum->activeNodes)
        {
            shardServer = configState.GetShardServer(*itNodeID);
            ASSERT(shardServer != NULL);
            CONTEXT_TRANSPORT->AddConnection(*itNodeID, shardServer->endpoint);
        }
    }
    else
    {
        configQuorum->GetVolatileActiveNodes(activeNodes);
//        quorumProcessor->SetActiveNodes(activeNodes);

        // add nodes to CONTEXT_TRANSPORT
        FOREACH (itNodeID, configQuorum->activeNodes)
        {
            shardServer = configState.GetShardServer(*itNodeID);
            ASSERT(shardServer != NULL);
            CONTEXT_TRANSPORT->AddConnection(*itNodeID, shardServer->endpoint);
        }
    }

    databaseManager.SetShards(configQuorum->shards);
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

    buffer.Clear();
    totalMemory = 0;

    shardMemoryUsage = GetDatabaseManager()->GetEnvironment()->GetShardMemoryUsage();
    buffer.Appendf("Shard memory usage: %s\n", HUMAN_BYTES(shardMemoryUsage));
    totalMemory += shardMemoryUsage;

    logSegmentMemoryUsage = GetDatabaseManager()->GetEnvironment()->GetLogSegmentMemoryUsage();
    buffer.Appendf("Log segment memory usage: %s\n", HUMAN_BYTES(logSegmentMemoryUsage));
    totalMemory += logSegmentMemoryUsage;

    buffer.Appendf("Storage cache usage: %s\n", HUMAN_BYTES(StoragePageCache::GetSize()));
    totalMemory += StoragePageCache::GetSize();

    buffer.Appendf("Client request cache usage: %s\n", HUMAN_BYTES(REQUEST_CACHE->GetMemorySize()));
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
    buffer.Appendf("Message cache usage: %s\n", HUMAN_BYTES(quorumMessageCacheSize));
    totalMemory += quorumMessageCacheSize;

    buffer.Appendf("Message list usage: %s\n", HUMAN_BYTES(quorumMessageListSize));
    totalMemory += quorumMessageListSize;

    buffer.Appendf("Shard append state usage: %s\n", HUMAN_BYTES(quorumAppendStateSize));
    totalMemory += quorumAppendStateSize;

    buffer.Appendf("Shard quorum context usage: %s\n", HUMAN_BYTES(quorumContextSize));
    totalMemory += quorumContextSize;

    connectionMemoryUsage = GetShardServerApp()->GetMemoryUsage();
    buffer.Appendf("Connection memory usage: %s\n", HUMAN_BYTES(connectionMemoryUsage));
    totalMemory += connectionMemoryUsage;

    buffer.Appendf("Total memory usage: %s\n", HUMAN_BYTES(totalMemory));
    buffer.Appendf("Total memory usage reported by system: %s\n", HUMAN_BYTES(GetProcessMemoryUsage()));
}

unsigned ShardServer::GetNumSDBPClients()
{
    return shardServerApp->GetNumSDBPClients();
}
