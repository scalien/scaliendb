#include "ConfigServer.h"
#include "ConfigMessage.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequestCache.h"
#include "ConfigQuorumProcessor.h"

void ConfigServer::Init()
{
    unsigned        numConfigServers;
    int64_t         nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;

    REQUEST_CACHE->Init(configFile.GetIntValue("requestCache.size", 100));
 
    databaseManager.Init();
 
    REPLICATION_CONFIG->Init(databaseManager.GetDatabase()->GetTable("system"));
    
    runID = REPLICATION_CONFIG->GetRunID();
    runID += 1;
    REPLICATION_CONFIG->SetRunID(runID);
    REPLICATION_CONFIG->Commit();
   
    nodeID = configFile.GetIntValue("nodeID", -1);
    if (nodeID < 0)
        ASSERT_FAIL();
    
    CONTEXT_TRANSPORT->SetSelfNodeID(nodeID);
    REPLICATION_CONFIG->SetNodeID(nodeID);
    
    CONTEXT_TRANSPORT->SetClusterContext(this);
        
    // connect to the configServer nodes
    numConfigServers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numConfigServers; nodeID++)
    {
        str = configFile.GetListValue("controllers", (int) nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
    }

    quorumProcessor.Init(this, numConfigServers, databaseManager.GetDatabase()->GetTable("paxos"));
    primaryLeaseManager.Init(this);
    activationManager.Init(this);
}

void ConfigServer::Shutdown()
{
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    REQUEST_CACHE->Shutdown();
    heartbeatManager.Shutdown();
    databaseManager.Shutdown();
    primaryLeaseManager.Shutdown();
}

uint64_t ConfigServer::GetNodeID()
{
    return MY_NODEID;
}

ConfigDatabaseManager* ConfigServer::GetDatabaseManager()
{
    return &databaseManager;
}

ConfigQuorumProcessor* ConfigServer::GetQuorumProcessor()
{
    return &quorumProcessor;
}

ConfigHeartbeatManager* ConfigServer::GetHeartbeatManager()
{
    return &heartbeatManager;
}

ConfigActivationManager* ConfigServer::GetActivationManager()
{
    return &activationManager;
}

void ConfigServer::OnConfigStateChanged()
{
    activationManager.UpdateTimeout();
    quorumProcessor.UpdateListeners();
}

bool ConfigServer::IsValidClientRequest(ClientRequest* request)
{
     return request->IsControllerRequest();
}

void ConfigServer::OnClientRequest(ClientRequest* request)
{
    quorumProcessor.OnClientRequest(request);
}

void ConfigServer::OnClientClose(ClientSession* session)
{
    quorumProcessor.OnClientClose(session);
}

void ConfigServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& message)
{
//    heartbeatManager.RegisterHeartbeat(nodeID);

    switch (message.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_HEARTBEAT:
            heartbeatManager.OnHeartbeatMessage(message);
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_REQUEST_LEASE:
            primaryLeaseManager.OnRequestPrimaryLease(message);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            ASSERT_FAIL();
        default:
            ASSERT_FAIL();
    }
}

void ConfigServer::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
    ClusterMessage      clusterMessage;
    ConfigShardServer*  shardServer;
    ConfigState*        configState;
    
    configState = databaseManager.GetConfigState();
    
    // if it's a shard server, send the configState
    if (nodeID >= CONFIG_MIN_SHARD_NODE_ID)
    {
        // TODO: send shutdown message in case of bad configuration
        shardServer = configState->GetShardServer(nodeID);
        if (shardServer == NULL)
            return;
        
        if (shardServer->endpoint != endpoint)
        {
            Log_Message("Badly configured shard server trying to connect, nodeID: %U, endpoint: %s",
             nodeID, endpoint.ToString());

            return;
        }
        
        clusterMessage.SetConfigState(*configState);
        CONTEXT_TRANSPORT->SendClusterMessage(nodeID, clusterMessage);
        
        Log_Message("[%s] Shard server connected (%U)", endpoint.ToString(), nodeID);
    }
}

bool ConfigServer::OnAwaitingNodeID(Endpoint endpoint)
{   
    ConfigShardServer*              shardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  clusterMessage;
    ConfigState*                    configState;

    if (!quorumProcessor.IsMaster())
        return true; // drop connection
    
    configState = databaseManager.GetConfigState();
    
    // look for existing endpoint
    shardServers = &configState->shardServers;
    for (shardServer = shardServers->First(); shardServer != NULL; shardServer = shardServers->Next(shardServer))
    {
        if (shardServer->endpoint == endpoint)
        {
            // tell ContextTransport that this connection has a new nodeID
            CONTEXT_TRANSPORT->SetConnectionNodeID(endpoint, shardServer->nodeID);       
            
            // tell the shard server
            clusterMessage.SetNodeID(shardServer->nodeID);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            
            // send config state
            clusterMessage.SetConfigState(*configState);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            return false;
        }
    }

    quorumProcessor.TryRegisterShardServer(endpoint);
    return false;
}
