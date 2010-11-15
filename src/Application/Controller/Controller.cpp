#include "Controller.h"
#include "ConfigMessage.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequestCache.h"
#include "ConfigQuorumProcessor.h"

void Controller::Init()
{
    unsigned        numControllers;
    int64_t         nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;

    REQUEST_CACHE->Init(configFile.GetIntValue("requestCache.size", 100));
 
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
        str = configFile.GetListValue("controllers", (int) nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
    }

    configState.Init();
    quorumProcessor.Init(this, numControllers, systemDatabase->GetTable("paxos"));    
    ReadConfigState();

    configStatePaxosID = 0;
}

void Controller::Shutdown()
{
    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    REQUEST_CACHE->Shutdown();
    databaseEnv.Close();
}

int64_t Controller::GetMaster()
{
    return quorumProcessor.GetMaster();
}

uint64_t Controller::GetNodeID()
{
    return MY_NODEID;
}

ConfigState* Controller::GetConfigState()
{
    return &configState;
}

void Controller::OnLeaseTimeout()
{
    configState.hasMaster = false;
    configState.masterID = 0;    
}

void Controller::OnIsLeader()
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

void Controller::OnAppend(uint64_t /*paxosID*/, ConfigMessage& message, bool ownAppend)
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
    UpdateActivationTimeout();
    
    if (configContext.IsLeader())
        UpdateListeners();

    if (ownAppend)
    {
        assert(configMessages.GetLength() > 0);
        assert(configMessages.First()->type == message.type);
        if (configMessages.First()->fromClient)
            SendClientResponse(message);
        configMessages.Delete(configMessages.First());
    }
    
    if (configMessages.GetLength() > 0)
        TryAppend();
}

void Controller::OnStartCatchup()
{
    CatchupMessage    msg;
    
    if (isCatchingUp || !configContext.IsLeaseKnown())
        return;
    
    configContext.StopReplication();
    
    msg.CatchupRequest(MY_NODEID, configContext.GetQuorumID());
    
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

void Controller::OnClientClose(ClientSession* session)
{
    quorumProcessor.OnClientClose(session);
}

void Controller::OnClusterMessage(uint64_t nodeID, ClusterMessage& message)
{
    heartbeatManager.RegisterHeartbeat(nodeID);

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
        
        Log_Message("[%s] Shard server connected (%" PRIu64 ")", endpoint.ToString(), nodeID);
    }
}

bool Controller::OnAwaitingNodeID(Endpoint endpoint)
{   
    ConfigShardServer*              shardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  clusterMessage;

    if (!quorumProcessor.IsMaster())
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

void Controller::TryActivatingShardServer(uint64_t nodeID)
{
    uint64_t            paxosID;
    uint64_t*           itNodeID;
    ConfigQuorum*       itQuorum;
    ConfigShardServer*  shardServer;
    uint64_t            now;

    shardServer = configState.GetShardServer(nodeID);

    now = EventLoop::Now();
    if (now < shardServer->nextActivationTime)
    {
        Log_Trace("not trying activation due to penalty");
        return;
    }

    FOREACH(itQuorum, configState.quorums)
    {
        if (itQuorum->isActivatingNode)
            continue;
            
        if (!itQuorum->hasPrimary)
            continue;

        FOREACH(itNodeID, itQuorum->inactiveNodes)
        {
            if (*itNodeID == nodeID)
            {
                paxosID = QuorumPaxosID::GetPaxosID(shardServer->quorumPaxosIDs, itQuorum->quorumID);
                if (paxosID >= (itQuorum->paxosID - RLOG_REACTIVATION_DIFF))
                {
                    // the shard server is "almost caught up", start the activation process
                    itQuorum->isActivatingNode = true;
                    itQuorum->activatingNodeID = nodeID;
                    itQuorum->isWatchingPaxosID = false;
                    itQuorum->isReplicatingActivation = false;
                    itQuorum->configID++;
                    itQuorum->activationExpireTime = now + PAXOSLEASE_MAX_LEASE_TIME;
                    UpdateActivationTimeout();

                    Log_Message("Activation started for quorum %" PRIu64 " and shard server %" PRIu64 "",
                     itQuorum->quorumID, itQuorum->activatingNodeID);
                }
            }
        }
    }    
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

void Controller::UpdateListeners()
{
    ClientRequest*                  itRequest;
    ConfigShardServer*              itShardServer;
    ConfigState::ShardServerList*   shardServers;
    ClusterMessage                  message;
    
    // update clients
    for (itRequest = listenRequests.First(); itRequest != NULL; itRequest = listenRequests.Next(itRequest))
    {
        itRequest->response.ConfigStateResponse(configState);
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
