#include "Controller.h"
#include "ConfigMessage.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClusterMessage.h"

void Controller::Init()
{
    unsigned        numControllers;
    int64_t         nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;
    
    primaryLeaseTimeout.SetCallable(MFUNC(Controller, OnPrimaryLeaseTimeout));
 
    systemDatabase.Open(configFile.GetValue("database.dir", "db"), "system");
    REPLICATION_CONFIG->Init(systemDatabase.GetTable("system"));
    
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
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
    }

    configState.Init();
    configContext.Init(this, numControllers, systemDatabase.GetTable("paxos"));
    CONTEXT_TRANSPORT->AddQuorumContext(&configContext);
    
    ReadConfigState();
}

void Controller::Shutdown()
{
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    systemDatabase.Close();
}

int64_t Controller::GetMaster()
{
    return (int64_t) configContext.GetLeader();
}

uint64_t Controller::GetNodeID()
{
    return REPLICATION_CONFIG->GetNodeID();
}

ConfigState* Controller::GetConfigState()
{
    return &configState;
}

void Controller::OnLearnLease()
{
    bool updateListeners;
    
    updateListeners = false;
    if (!configState.hasMaster)
        updateListeners = true;

    configState.hasMaster = true;
    configState.masterID = GetMaster();

    if (updateListeners && (uint64_t) GetMaster() == GetNodeID())
        UpdateListeners();
}

void Controller::OnLeaseTimeout()
{
    ConfigMessage*  itMessage;
    ClientRequest*  itRequest;
    
    configState.hasMaster = false;
    configState.masterID = 0;

    for (itRequest = requests.First(); itRequest != NULL; itRequest = requests.First())
    {
        requests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }
    assert(requests.GetLength() == 0);
    
    for (itRequest = listenRequests.First(); itRequest != NULL; itRequest = listenRequests.First())
    {
        listenRequests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }
    assert(listenRequests.GetLength() == 0);

    for (itMessage = configMessages.First(); itMessage != NULL; itMessage = configMessages.Delete(itMessage));
    assert(configMessages.GetLength() == 0);
}

void Controller::OnAppend(ConfigMessage& message, bool ownAppend)
{
    ClusterMessage  clusterMessage;
    
    if (message.type == CONFIGMESSAGE_REGISTER_SHARDSERVER)
    {
        // tell ContextTransport that this connection has a new nodeID
        CONTEXT_TRANSPORT->SetConnectionNodeID(message.endpoint, message.nodeID);       
        // tell the shard server
        clusterMessage.SetNodeID(message.nodeID);
        CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, clusterMessage);
    }
    
    configState.OnMessage(message);
    WriteConfigState();
    
    if (ownAppend)
    {
        assert(configMessages.GetLength() > 0);
        assert(configMessages.First()->type == message.type);
        if (configMessages.First()->fromClient)
            SendClientResponse(message);
        configMessages.Delete(configMessages.First());
    }
    
    if (configContext.IsLeader())
        UpdateListeners();    
}

bool Controller::IsValidClientRequest(ClientRequest* request)
{
     return request->IsControllerRequest();
}

void Controller::OnClientRequest(ClientRequest* request)
{
    ConfigMessage*  message;

    if (request->type == CLIENTREQUEST_GET_MASTER)
    {
        if (configContext.IsLeaderKnown())
            request->response.Number(configContext.GetLeader());
        else
            request->response.NoService();
            
        request->OnComplete();
        return;
    }
    else if (request->type == CLIENTREQUEST_GET_CONFIG_STATE)
    {
        listenRequests.Append(request);
        request->response.GetConfigStateResponse(&configState);
        request->OnComplete(false);
        return;
    }
    
    message = new ConfigMessage;
    FromClientRequest(request, message);
    
    if (!configState.CompleteMessage(*message))
    {
        delete message;
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    requests.Append(request);
    configMessages.Append(message);
    TryAppend();
}

void Controller::OnClientClose(ClientSession* session)
{
    ClientRequest*  it;
    ClientRequest*  next;
    
    for (it = listenRequests.First(); it != NULL; it = next)
    {
        next = listenRequests.Next(it);
        if (it->session == session)
        {
            listenRequests.Remove(it);
            it->OnComplete();
        }
    }
}

void Controller::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& message)
{
    if (!configContext.IsLeader())
        return; 

    switch (message.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_REQUEST_LEASE:
            OnRequestLease(message);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            ASSERT_FAIL();
    }
}

void Controller::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
    ClusterMessage      clusterMessage;
    ConfigShardServer*  shardServer;
    
    // if it's a shard server, send the configState
    if (nodeID >= CONFIG_MIN_SHARD_NODE_ID)
    {
        // TODO: send shutdown message in case of bad configuration
        shardServer = configState.GetShardServer(nodeID);
        if (shardServer == NULL)
            return;
        
        if (shardServer->endpoint != endpoint)
        {
            Log_Message("Badly configured shard server trying to connect"
             ", nodeID: %" PRIu64
             ", endpoint: %s",
             nodeID, endpoint.ToString());

            return;
        }
        
        clusterMessage.SetConfigState(&configState);
        CONTEXT_TRANSPORT->SendClusterMessage(nodeID, clusterMessage);
    }
}

bool Controller::OnAwaitingNodeID(Endpoint endpoint)
{   
    ConfigShardServer*              shardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  clusterMessage;

    if (!configContext.IsLeader())
        return true; // drop connection
    
    // look for existing endpoint
    shardServers = &configState.shardServers;
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
            clusterMessage.SetConfigState(&configState);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            return false;
        }
    }

    TryRegisterShardServer(endpoint);
    return false;
}

void Controller::TryAppend()
{
    if (!configContext.IsAppending())
        configContext.Append(configMessages.First());
}

void Controller::FromClientRequest(ClientRequest* request, ConfigMessage* message)
{
    message->fromClient = true;
    
    switch (request->type)
    {
        case CLIENTREQUEST_CREATE_QUORUM:
            message->type = CONFIGMESSAGE_CREATE_QUORUM;
            message->nodes = request->nodes;
            return;
        case CLIENTREQUEST_CREATE_DATABASE:
            message->type = CONFIGMESSAGE_CREATE_DATABASE;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_RENAME_DATABASE:
            message->type = CONFIGMESSAGE_RENAME_DATABASE;
            message->databaseID = request->databaseID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_DELETE_DATABASE:
            message->type = CONFIGMESSAGE_DELETE_DATABASE;
            message->databaseID = request->databaseID;
            return;
        case CLIENTREQUEST_CREATE_TABLE:
            message->type = CONFIGMESSAGE_CREATE_TABLE;
            message->databaseID = request->databaseID;
            message->quorumID = request->quorumID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_RENAME_TABLE:
            message->type = CONFIGMESSAGE_RENAME_TABLE;
            message->databaseID = request->databaseID;
            message->tableID = request->tableID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_DELETE_TABLE:
            message->type = CONFIGMESSAGE_DELETE_TABLE;
            message->databaseID = request->databaseID;
            message->tableID = request->tableID;
            return;
        default:
            ASSERT_FAIL();
    }
}

void Controller::ToClientResponse(ConfigMessage* message, ClientResponse* response)
{
    switch (response->request->type)
    {
        case CLIENTREQUEST_CREATE_QUORUM:
            response->Number(message->quorumID);
            return;
        case CLIENTREQUEST_CREATE_DATABASE:
            response->Number(message->databaseID);
            return;
        case CLIENTREQUEST_RENAME_DATABASE:
            response->OK();
            return;
        case CLIENTREQUEST_DELETE_DATABASE:
            response->OK();
            return;
        case CLIENTREQUEST_CREATE_TABLE:
            response->Number(message->tableID);
            return;
        case CLIENTREQUEST_RENAME_TABLE:
            response->OK();
            return;
        case CLIENTREQUEST_DELETE_TABLE:
            response->OK();
            return;
        default:
            ASSERT_FAIL();
    }
}

void Controller::OnPrimaryLeaseTimeout()
{
    uint64_t        now;
    ConfigQuorum*   quorum;
    PrimaryLease*   itLease;
    
    now = EventLoop::Now();

    for (itLease = primaryLeases.First(); itLease != NULL; /* advanced in body */)
    {
        if (itLease->expireTime < now)
        {
            quorum = configState.GetQuorum(itLease->quorumID);
            quorum->hasPrimary = false;
            quorum->primaryID = 0;
            itLease = primaryLeases.Delete(itLease);
        }
        else
            break;
    }

    UpdatePrimaryLeaseTimer();
    UpdateListeners();
}

void Controller::TryRegisterShardServer(Endpoint& endpoint)
{
    ConfigMessage*      message;

    message = new ConfigMessage;
    message->fromClient = false;
    message->RegisterShardServer(0, endpoint);
    if (!configState.CompleteMessage(*message))
        ASSERT_FAIL();

    configMessages.Append(message);
    TryAppend();
}

void Controller::ReadConfigState()
{
    bool        ret;
    ReadBuffer  value;
    
    ret =  true;
    ret &= systemDatabase.GetTable("config")->Get(ReadBuffer("state"), value);
    ret &= configState.Read(value);
    if (!ret)
        Log_Message("Starting with empty database...");
    
    Log_Trace("%.*s", P(&value));
}

void Controller::WriteConfigState()
{
    configState.Write(configStateBuffer);
    systemDatabase.GetTable("config")->Set(ReadBuffer("state"), ReadBuffer(configStateBuffer));
    systemDatabase.Commit();
}

void Controller::SendClientResponse(ConfigMessage& message)
{
    ClientRequest* request;
    
    assert(requests.GetLength() > 0);
    
    request = requests.First();
    requests.Remove(request);
    
    ToClientResponse(&message, &request->response);
    request->OnComplete();
    
}

void Controller::OnRequestLease(ClusterMessage& message)
{
    uint64_t*       it;
    ConfigQuorum*   quorum;
    
    quorum = configState.GetQuorum(message.quorumID);
    
    if (quorum == NULL)
    {
        Log_Trace("nodeID " PRIu64 " requesting lease for non-existing quorum %" PRIu64 "",
         message.nodeID, message.quorumID);
        return;
    }
    
    if (quorum->hasPrimary)
    {
        if (quorum->primaryID == message.nodeID)
            ExtendPrimaryLease(*quorum, message);
        return;
    }
    
    for (it = quorum->activeNodes.First(); it != NULL; it = quorum->activeNodes.Next(it))
    {
        if (*it == message.nodeID)
            break;
    }
    
    if (it == NULL)
    {
        Log_Trace("nodeID " PRIu64 " requesting lease but not active member or quorum %" PRIu64 "",
         message.nodeID, message.quorumID);
        return;
    }
    
    AssignPrimaryLease(*quorum, message);
}

void Controller::AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    unsigned        duration;
    PrimaryLease*   primaryLease;
    ClusterMessage  response;

    quorum.hasPrimary = true;
    quorum.primaryID = message.nodeID;

    primaryLease = new PrimaryLease;
    primaryLease->quorumID = quorum.quorumID;
    primaryLease->nodeID = quorum.primaryID;
    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
    primaryLease->expireTime = EventLoop::Now() + duration;
    primaryLeases.Add(primaryLease);

    UpdatePrimaryLeaseTimer();

    response.ReceiveLease(message.nodeID, message.quorumID, message.proposalID, duration);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);
}

void Controller::ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    unsigned        duration;
    PrimaryLease*   it;
    ClusterMessage  response;

    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);

    for (it = primaryLeases.First(); it != NULL; it = primaryLeases.Next(it))
    {
        if (it->quorumID == quorum.quorumID)
            break;
    }
    
    assert(it != NULL);
    
    primaryLeases.Remove(it);
    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
    it->expireTime = EventLoop::Now() + duration;
    primaryLeases.Add(it);
    UpdatePrimaryLeaseTimer();

    response.ReceiveLease(message.nodeID, message.quorumID, message.proposalID, duration);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);
}

void Controller::UpdatePrimaryLeaseTimer()
{
    PrimaryLease* primaryLease;

    primaryLease = primaryLeases.First();
    if (!primaryLease)
        return;
    
    if (primaryLease->expireTime < primaryLeaseTimeout.GetExpireTime())
    {
        EventLoop::Remove(&primaryLeaseTimeout);
        primaryLeaseTimeout.SetExpireTime(primaryLease->expireTime);
        EventLoop::Add(&primaryLeaseTimeout);
    }
}

void Controller::UpdateListeners()
{
    ClientRequest*                  itRequest;
    ConfigShardServer*              itShardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  message;
    
    // update clients
    for (itRequest = listenRequests.First(); itRequest != NULL; itRequest = listenRequests.Next(itRequest))
    {
        itRequest->response.GetConfigStateResponse(&configState);
        itRequest->OnComplete(false);
    }
    
    // update shard servers
    message.SetConfigState(&configState);
    shardServers = &configState.shardServers;
    for (itShardServer = shardServers->First(); 
     itShardServer != NULL; 
     itShardServer = shardServers->Next(itShardServer))
    {
        CONTEXT_TRANSPORT->SendClusterMessage(itShardServer->nodeID, message);
    }
}
