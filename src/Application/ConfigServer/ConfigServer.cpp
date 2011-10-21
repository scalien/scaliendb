#include "ConfigServer.h"
#include "ConfigServerApp.h"
#include "ConfigMessage.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequestCache.h"
#include "ConfigQuorumProcessor.h"

#define CONFIG_STATE    (databaseManager.GetConfigState())

ConfigServer::ConfigServer()
{
    broadcastHTTPEndpoint.SetCallable(MFUNC(ConfigServer, OnBroadcastHTTPEndpoint));
    broadcastHTTPEndpoint.SetDelay(BROADCAST_HTTP_ENDPOINT_DELAY);
}

void ConfigServer::Init(ConfigServerApp* app)
{
    unsigned        numConfigServers;
    int64_t         nodeID;
    uint64_t        runID, nodeID_;
    const char*     str;
    Endpoint        endpoint;

    configServerApp = app;

    REQUEST_CACHE->Init(configFile.GetIntValue("requestCache.size", 100));
 
    databaseManager.Init();
 
    REPLICATION_CONFIG->Init(databaseManager.GetSystemShard());
    
    runID = REPLICATION_CONFIG->GetRunID();
    runID += 1;
    REPLICATION_CONFIG->SetRunID(runID);
    REPLICATION_CONFIG->Commit();
   
    nodeID = configFile.GetIntValue("nodeID", -1);
    if (nodeID < 0)
        STOP_FAIL(1, "Missing \"nodeID\" in config file!");
    
    CONTEXT_TRANSPORT->SetSelfNodeID(nodeID);
    REPLICATION_CONFIG->SetNodeID(nodeID);
    Log_Message("My nodeID is %U", REPLICATION_CONFIG->GetNodeID());

    CONTEXT_TRANSPORT->SetClusterContext(this);
    CONTEXT_TRANSPORT->SetClusterID(REPLICATION_CONFIG->GetClusterID());
        
    // connect to the configServer nodes
    if (configFile.GetValue("controllers", NULL) == NULL)
        STOP_FAIL(1, "Missing \"controllers\" in config file!");
    numConfigServers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numConfigServers; nodeID++)
    {
        nodeID_ = nodeID;
        configServers.Append(nodeID_);
        str = configFile.GetListValue("controllers", (int) nodeID, "");
        endpoint.Set(str, true);

        // config file sanity check
        if (nodeID_ == CONTEXT_TRANSPORT->GetSelfNodeID() && 
         endpoint != CONTEXT_TRANSPORT->GetSelfEndpoint())
            STOP_FAIL(1, "Invalid \"nodeID\" or \"endpoint\" in config file!");
        
        CONTEXT_TRANSPORT->AddConnection(nodeID, endpoint);
    }

    quorumProcessor.Init(this, numConfigServers,
     databaseManager.GetQuorumPaxosShard(), databaseManager.GetQuorumLogShard());
    heartbeatManager.Init(this);
    primaryLeaseManager.Init(this);
    activationManager.Init(this);
    
    httpEndpoint.Set(configFile.GetValue("endpoint", ""));
    httpEndpoint.SetPort(configFile.GetIntValue("http.port", 8080));
    EventLoop::Add(&broadcastHTTPEndpoint);
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

ConfigPrimaryLeaseManager* ConfigServer::GetPrimaryLeaseManager()
{
    return &primaryLeaseManager;
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
    Endpoint    endpoint;
    
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
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMPLETE:
            quorumProcessor.OnShardMigrationComplete(message);
            break;
        case CLUSTERMESSAGE_HELLO:
            // TODO:
            break;
        case CLUSTERMESSAGE_HTTP_ENDPOINT:
            endpoint.Set(message.endpoint);
            httpEndpoints.Set(message.nodeID, endpoint);
            break;
        default:
            ASSERT_FAIL();
    }
}

void ConfigServer::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
    ClusterMessage      clusterMessage;
    ConfigShardServer*  shardServer;
    ConfigController*   controller;

    if (IS_CONTROLLER(nodeID))
    {
        if (REPLICATION_CONFIG->GetClusterID() == 0 && CONTEXT_TRANSPORT->GetClusterID() > 0)
        {
            // another controller connected, it has a clusterID, I don't, so set mine
            // the other side's was set in CONTEXT_TRANSPORT at a layer below,
            // more specifically in ClusterConnection.cpp:224

            REPLICATION_CONFIG->SetClusterID(CONTEXT_TRANSPORT->GetClusterID());
            REPLICATION_CONFIG->Commit();
        }

        controller = CONFIG_STATE->controllers.Get(nodeID);
        ASSERT(controller);
        controller->isConnected = true;

        quorumProcessor.UpdateListeners(/*force=*/true);
        return;
    }
    
    shardServer = CONFIG_STATE->GetShardServer(nodeID);

    if (shardServer == NULL)
    {
        Log_Message("Badly configured or unregistered shard server trying to connect, no such nodeID: %U, endpoint: %s",
         nodeID, endpoint.ToString());
        CONTEXT_TRANSPORT->DropConnection(nodeID);
        return;
    }

    if (shardServer->endpoint != endpoint)
    {
        Log_Debug("%s != ", shardServer->endpoint.ToString());
        Log_Debug("!= %s", endpoint.ToString());
        // shard server endpoint changed, update ConfigState
        quorumProcessor.TryUpdateShardServer(nodeID, endpoint);
        CONTEXT_TRANSPORT->DropConnection(nodeID);
        return;
    }
    
    if (quorumProcessor.IsMaster())
    {
        clusterMessage.SetConfigState(*CONFIG_STATE);
        CONTEXT_TRANSPORT->SendClusterMessage(nodeID, clusterMessage);
        
        Log_Message("[%s] Shard server connected (%U)", endpoint.ToString(), nodeID);
    }
}

void ConfigServer::OnConnectionEnd(uint64_t nodeID, Endpoint /*endpoint*/)
{
    ConfigController* controller;
    
    if (IS_CONTROLLER(nodeID))
    {
        controller = CONFIG_STATE->controllers.Get(nodeID);
        ASSERT(controller);
        controller->isConnected = false;
        
        quorumProcessor.UpdateListeners(/*force=*/true);
        return;
    }
}

bool ConfigServer::OnAwaitingNodeID(Endpoint endpoint)
{   
    ConfigShardServer*              shardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  clusterMessage;

    if (!quorumProcessor.IsMaster())
        return true; // drop connection
        
    // look for existing endpoint
    shardServers = &CONFIG_STATE->shardServers;
    FOREACH (shardServer, *shardServers)
    {
        if (shardServer->endpoint == endpoint)
        {
            activationManager.TryDeactivateShardServer(shardServer->nodeID, /* force */ true);

            // tell ContextTransport that this connection has a new nodeID
            CONTEXT_TRANSPORT->SetConnectionNodeID(endpoint, shardServer->nodeID);       
            
            // tell the shard server
            clusterMessage.SetNodeID(REPLICATION_CONFIG->GetClusterID(), shardServer->nodeID);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            
            // send config state
            clusterMessage.SetConfigState(*CONFIG_STATE);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            return false;
        }
    }

    quorumProcessor.TryRegisterShardServer(endpoint);
    return false;
}

bool ConfigServer::GetControllerHTTPEndpoint(uint64_t nodeID, Endpoint& endpoint)
{
    return httpEndpoints.Get(nodeID, endpoint);
}

void ConfigServer::GetHTTPEndpoint(Endpoint& endpoint)
{
    endpoint = httpEndpoint;
}

unsigned ConfigServer::GetNumSDBPClients()
{
    return configServerApp->GetNumSDBPClients();
}

void ConfigServer::OnBroadcastHTTPEndpoint()
{
    uint64_t*       itNodeID;
    ClusterMessage  clusterMessage;
    
    clusterMessage.HttpEndpoint(MY_NODEID, httpEndpoint.ToString());

    FOREACH (itNodeID, configServers)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, clusterMessage);
    
    EventLoop::Add(&broadcastHTTPEndpoint);
}

uint64_t Hash(uint64_t i)
{
    return i;
}
