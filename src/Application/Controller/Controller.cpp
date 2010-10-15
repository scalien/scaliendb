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
    uint64_t        logCacheSize;
    const char*     str;
    Endpoint        endpoint;
    
    primaryLeaseTimeout.SetCallable(MFUNC(Controller, OnPrimaryLeaseTimeout));

    heartbeatTimeout.SetCallable(MFUNC(Controller, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(1000);
    EventLoop::Add(&heartbeatTimeout);
 
    databaseEnv.InitCache(configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_CACHE_SIZE));
    databaseEnv.Open(configFile.GetValue("database.dir", "db"));
    systemDatabase = databaseEnv.GetDatabase("system");
    REPLICATION_CONFIG->Init(systemDatabase->GetTable("system"));
    
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
    
    isCatchingUp = false;
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
    }

    configState.Init();
    logCacheSize = configFile.GetIntValue("rlog.cacheSize", DEFAULT_RLOG_CACHE_SIZE);
    configContext.Init(this, numControllers, systemDatabase->GetTable("paxos"), logCacheSize);
    CONTEXT_TRANSPORT->AddQuorumContext(&configContext);
    
    ReadConfigState();
}

void Controller::Shutdown()
{
    heartbeats.DeleteList();
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    databaseEnv.Close();
}

int64_t Controller::GetMaster()
{
    if (!configContext.IsLeaseKnown())
        return -1;
    return (int64_t) configContext.GetLeaseOwner();
}

uint64_t Controller::GetNodeID()
{
    return REPLICATION_CONFIG->GetNodeID();
}

uint64_t Controller::GetReplicationRound()
{
    return configContext.GetPaxosID();
}

ConfigState* Controller::GetConfigState()
{
    return &configState;
}

void Controller::RegisterHeartbeat(uint64_t nodeID)
{
    Heartbeat       *it;
    uint64_t        now;
    
    Log_Trace("Got heartbeat from %" PRIu64 "", nodeID);
    
    now = Now();
    
    FOREACH(it, heartbeats)
    {
        if (it->nodeID == nodeID)
        {
            heartbeats.Remove(it);
            it->expireTime = now + HEARTBEAT_EXPIRE_TIME;
            heartbeats.Add(it);
            return;
        }
    }
    
    it = new Heartbeat();
    it->nodeID = nodeID;
    it->expireTime = now + HEARTBEAT_EXPIRE_TIME;
    heartbeats.Add(it);
}

bool Controller::HasHeartbeat(uint64_t nodeID)
{
    Heartbeat* it;
    FOREACH(it, heartbeats)
    {
        if (it->nodeID == nodeID)
            return true;
    }

    return false;
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
    
    // catchup issue:
    // when OnAppend() is called, the paxosID is already incremented in ReplicatedLog
    // (configContext()->GetPaxosID() - 1) is the round of this message
    // if this paxosID is smaller or equal to configStatePaxosID, that means
    // our state already includes the mutations due to this round
    if ((configContext.GetPaxosID() - 1) <= configStatePaxosID)
        return;
    
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
        
    if (configContext.IsLeaseOwner())
        UpdateListeners();
    
    if (configMessages.GetLength() > 0)
        TryAppend();
}

void Controller::OnStartCatchup()
{
    CatchupMessage    msg;
    
    if (isCatchingUp || !configContext.IsLeaseKnown())
        return;
    
    configContext.StopReplication();
    
    msg.CatchupRequest(REPLICATION_CONFIG->GetNodeID(), configContext.GetQuorumID());
    
    CONTEXT_TRANSPORT->SendQuorumMessage(
     configContext.GetLeaseOwner(), configContext.GetQuorumID(), msg);
     
    isCatchingUp = true;
    
    Log_Message("Catchup started from node %" PRIu64 "", configContext.GetLeaseOwner());
}

void Controller::OnCatchupMessage(CatchupMessage& imsg)
{
    CatchupMessage  omsg;
    Buffer          buffer;
    ReadBuffer      key;
    ReadBuffer      value;
    bool            hasMaster;
    uint64_t        masterID;
    
    switch (imsg.type)
    {
        case CATCHUPMESSAGE_REQUEST:
            if (!configContext.IsLeader())
                return;
            assert(imsg.quorumID == configContext.GetQuorumID());
            // send configState
            key.Wrap("state");
            configState.Write(buffer);
            value.Wrap(buffer);
            omsg.KeyValue(key, value);
            CONTEXT_TRANSPORT->SendQuorumMessage(imsg.nodeID, configContext.GetQuorumID(), omsg);
            omsg.Commit(configContext.GetPaxosID() - 1);
            // send the paxosID whose value is in the db
            CONTEXT_TRANSPORT->SendQuorumMessage(imsg.nodeID, configContext.GetQuorumID(), omsg);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            ASSERT_FAIL();
            break;
        case CATCHUPMESSAGE_KEYVALUE:
            if (!isCatchingUp)
                return;
            hasMaster = configState.hasMaster;
            masterID = configState.masterID;
            if (!configState.Read(imsg.value))
                ASSERT_FAIL();
            configState.hasMaster = hasMaster;
            configState.masterID = masterID;
            break;
        case CATCHUPMESSAGE_COMMIT:
            if (!isCatchingUp)
                return;
            configStatePaxosID = imsg.paxosID;
            WriteConfigState();
            configContext.OnCatchupComplete(imsg.paxosID);      // this commits
            isCatchingUp = false;
            configContext.ContinueReplication();
            Log_Message("Catchup complete");
            break;
        default:
            ASSERT_FAIL();
    }
}

bool Controller::IsValidClientRequest(ClientRequest* request)
{
     return request->IsControllerRequest();
}

void Controller::OnClientRequest(ClientRequest* request)
{
    ConfigMessage*  message;
    uint64_t*       itNodeID;

    if (request->type == CLIENTREQUEST_GET_MASTER)
    {
        if (configContext.IsLeaseKnown())
            request->response.Number(configContext.GetLeaseOwner());
        else
            request->response.NoService();
            
        request->OnComplete();
        return;
    }
    else if (request->type == CLIENTREQUEST_GET_CONFIG_STATE)
    {
        listenRequests.Append(request);
        request->response.GetConfigStateResponse(configState);
        request->OnComplete(false);
        return;
    }
    
    if (!configContext.IsLeader())
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    if (request->type == CLIENTREQUEST_CREATE_QUORUM)
    {
        // make sure all nodes are currently active
        FOREACH(itNodeID, request->nodes)
        {
            if (!HasHeartbeat(*itNodeID))
            {
                request->response.Failed();
                request->OnComplete();
                return;
            }
        }
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

void Controller::OnClusterMessage(uint64_t nodeID, ClusterMessage& message)
{
    if (!configContext.IsLeaseOwner())
        return;
        
    RegisterHeartbeat(nodeID);

    switch (message.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_HEARTBEAT:
            OnHeartbeat(message);
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            ASSERT_FAIL();
        case CLUSTERMESSAGE_REQUEST_LEASE:
            OnRequestLease(message);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            ASSERT_FAIL();
        default:
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
        
        clusterMessage.SetConfigState(configState);
        CONTEXT_TRANSPORT->SendClusterMessage(nodeID, clusterMessage);
    }
}

bool Controller::OnAwaitingNodeID(Endpoint endpoint)
{   
    ConfigShardServer*              shardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  clusterMessage;

    if (!configContext.IsLeaseOwner())
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
            clusterMessage.SetConfigState(configState);
            CONTEXT_TRANSPORT->SendClusterMessage(shardServer->nodeID, clusterMessage);
            return false;
        }
    }

    TryRegisterShardServer(endpoint);
    return false;
}

void Controller::TryAppend()
{
    assert(configMessages.GetLength() > 0);
    
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

void Controller::OnHeartbeatTimeout()
{
    uint64_t            now;
    Heartbeat*          itHeartbeat;
    ConfigShardServer*  itShardServer;
    
    // OnHeartbeatTimeout() arrives every 1000 msec
    
    now = Now();

    // first remove nodes from the heartbeats list which have
    // not sent a heartbeat in HEARTBEAT_EXPIRE_TIME
    for (itHeartbeat = heartbeats.First(); itHeartbeat != NULL; /* incremented in body */)
    {
        if (itHeartbeat->expireTime <= now)
        {
            CONTEXT_TRANSPORT->DropConnection(itHeartbeat->nodeID);
            Log_Trace("Removing node %" PRIu64 " from heartbeats", itHeartbeat->nodeID);
            itHeartbeat = heartbeats.Delete(itHeartbeat);
        }
        else
            break;
    }
    
    if (configContext.IsLeader())
    {
        FOREACH(itShardServer, configState.shardServers)
        {
            if (!HasHeartbeat(itShardServer->nodeID))
                TryDeactivateShardServer(itShardServer->nodeID);
            else
                TryActivateShardServer(itShardServer->nodeID);
        }
    }
    
    EventLoop::Add(&heartbeatTimeout);    
}

void Controller::TryDeactivateShardServer(uint64_t nodeID)
{
    bool                found;
    ConfigQuorum*       itQuorum;
    uint64_t*           itNodeID;
    ConfigMessage*      itConfigMessage;

    FOREACH(itQuorum, configState.quorums)
    {
        FOREACH(itNodeID, itQuorum->activeNodes)
        {
            if (*itNodeID == nodeID)
            {
                // make sure there is no corresponding deactivate message pending
                found = false;
                FOREACH(itConfigMessage, configMessages)
                {
                    if (itConfigMessage->type == CONFIGMESSAGE_DEACTIVATE_SHARDSERVER &&
                     itConfigMessage->quorumID == itQuorum->quorumID &&
                     itConfigMessage->nodeID == nodeID)
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                    continue;
                itConfigMessage = new ConfigMessage();
                itConfigMessage->fromClient = false;
                itConfigMessage->DeactivateShardServer(itQuorum->quorumID, nodeID);
                configMessages.Append(itConfigMessage);
                TryAppend();
            }
        }
    }
}

void Controller::TryActivateShardServer(uint64_t /*nodeID*/)
{
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
    bool                ret;
    ReadBuffer          value;
    int                 read;
    ConfigShardServer*  it;
    
    ret =  true;
    
    if (!systemDatabase->GetTable("config")->Get(ReadBuffer("state"), value))
    {
        Log_Message("Starting with empty database...");
        return;
    }
    
    read = value.Readf("%U:", &configStatePaxosID);
    if (read < 2)
        ASSERT_FAIL();
    
    value.Advance(read);
    
    if (!configState.Read(value))
        ASSERT_FAIL();

    FOREACH(it, configState.shardServers)
        RegisterHeartbeat(it->nodeID);
    
    Log_Trace("%.*s", P(&value));
}

void Controller::WriteConfigState()
{
    configStateBuffer.Writef("%U:", configStatePaxosID);
    configState.Write(configStateBuffer);
    systemDatabase->GetTable("config")->Set(ReadBuffer("state"), ReadBuffer(configStateBuffer));
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

void Controller::OnHeartbeat(ClusterMessage& message)
{
    QuorumPaxosID*  it;
    ConfigQuorum*   quorum;
    
    RegisterHeartbeat(message.nodeID);
    FOREACH(it, message.quorumPaxosIDs)
    {
        quorum = configState.GetQuorum(it->quorumID);
        if (!quorum)
            continue;
        if (quorum->paxosID < it->paxosID)
            quorum->paxosID = it->paxosID;
    }
}

void Controller::OnRequestLease(ClusterMessage& message)
{
    uint64_t*       it;
    ConfigQuorum*   quorum;
    
    quorum = configState.GetQuorum(message.quorumID);
    
    if (quorum == NULL)
    {
        Log_Trace("nodeID %" PRIu64 " requesting lease for non-existing quorum %" PRIu64 "",
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
        Log_Trace("nodeID %" PRIu64 " requesting lease but not active member or quorum %" PRIu64 "",
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
    
    // if there is no active primary lease timeout, or the timeout is later then the supposed first
    if (!primaryLeaseTimeout.IsActive() ||
     primaryLease->expireTime < primaryLeaseTimeout.GetExpireTime())
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
        itRequest->response.GetConfigStateResponse(configState);
        itRequest->OnComplete(false);
    }
    
    // update shard servers
    message.SetConfigState(configState);
    shardServers = &configState.shardServers;
    for (itShardServer = shardServers->First(); 
     itShardServer != NULL; 
     itShardServer = shardServers->Next(itShardServer))
    {
        CONTEXT_TRANSPORT->SendClusterMessage(itShardServer->nodeID, message);
    }
}
