#include "ConfigQuorumProcessor.h"
#include "Application/Common/ContextTransport.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "ConfigServer.h"
#include "ConfigHeartbeatManager.h"

#define CONFIG_STATE (configServer->GetDatabaseManager()->GetConfigState())

void ConfigQuorumProcessor::Init(ConfigServer* configServer_, unsigned numConfigServers,
 StorageShardProxy* quorumPaxosShard, StorageShardProxy* quorumLogShard)
{
    configServer = configServer_;
    
    isCatchingUp = false;
    configStateChecksum = 0;
    lastConfigChangeTime = 0;

    leaseKnown = false;
    leaseOwner = 0;

    quorumContext.Init(this, numConfigServers, quorumPaxosShard, quorumLogShard);    
    CONTEXT_TRANSPORT->AddQuorumContext(&quorumContext);

    onListenRequestTimeout.SetCallable(MFUNC(ConfigQuorumProcessor, OnListenRequestTimeout));
}

void ConfigQuorumProcessor::Shutdown()
{
    EventLoop::Remove(&onListenRequestTimeout);
    configMessages.DeleteList();
    requests.DeleteList();
    listenRequests.DeleteList();
}

bool ConfigQuorumProcessor::IsMaster()
{
    return quorumContext.IsLeaseOwner();
}

int64_t ConfigQuorumProcessor::GetMaster()
{
    if (!quorumContext.IsLeaseKnown())
        return -1;

    return (int64_t) quorumContext.GetLeaseOwner();
}

uint64_t ConfigQuorumProcessor::GetQuorumID()
{
    return quorumContext.GetQuorumID();
}

uint64_t ConfigQuorumProcessor::GetPaxosID()
{
    return quorumContext.GetPaxosID();
}

uint64_t ConfigQuorumProcessor::GetLastLearnChosenTime()
{
    return quorumContext.GetLastLearnChosenTime();
}

unsigned ConfigQuorumProcessor::GetNumConfigMessages()
{
    return configMessages.GetLength();
}

unsigned ConfigQuorumProcessor::GetNumRequests()
{
    return requests.GetLength();
}

unsigned ConfigQuorumProcessor::GetNumListenRequests()
{
    return listenRequests.GetLength();
}

void ConfigQuorumProcessor::TryAppend()
{
    ConfigMessage*  configMessage;
    ClientRequest*  request;
    
    if (quorumContext.IsAppending())
        return;

    FOREACH_FIRST (configMessage, configMessages)
    {    
        if (CONFIG_STATE->CompleteMessage(*configMessage))
        {
            quorumContext.Append(configMessage);
            break;
        }
        else
        {
            if (configMessage->fromClient)
            {
                ASSERT(requests.GetLength() > 0);
                request = requests.First();
                requests.Remove(request);
                request->response.Failed();
                request->OnComplete();
            }
            configMessages.Delete(configMessage);
        }
    }
}

void ConfigQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    uint64_t        srcNodeID, dstNodeID;
    uint64_t        srcQuorumID, dstQuorumID;
    uint64_t*       itNodeID;
    ConfigShard*    configShard;
    ConfigQuorum*   configQuorum;
    ConfigMessage*  configMessage;
    ClusterMessage  clusterMessage;
    Endpoint        endpoint;
    ReadBuffer      rb;

    if (request->type == CLIENTREQUEST_GET_MASTER)
    {
        if (quorumContext.IsLeaseKnown())
            request->response.Number(quorumContext.GetLeaseOwner());
        else
            request->response.NoService();
            
        request->OnComplete();
        return;
    }
    else if (request->type == CLIENTREQUEST_GET_MASTER_HTTP)
    {
        if (quorumContext.IsLeaseKnown())
        {
            configServer->GetControllerHTTPEndpoint(quorumContext.GetLeaseOwner(), endpoint);
            rb.Wrap(endpoint.ToString());
            request->response.Value(rb);
        }
        else
        {
            request->response.NoService();
        }
            
        request->OnComplete();
        return;
    }
    else if (request->type == CLIENTREQUEST_GET_CONFIG_STATE)
    {
        listenRequests.Append(request);
        if (quorumContext.IsLeader())
        {
            if (request->changeTimeout == 0 || 
             (request->paxosID != 0 && request->paxosID < CONFIG_STATE->paxosID))
            {
                // this is an immediate config state request
                request->response.ConfigStateResponse(*CONFIG_STATE);
                request->OnComplete(false);
            }
            
            // handle HTTP pollConfigState requests
            if (request->changeTimeout > 0)
            {
                if (!onListenRequestTimeout.IsActive())
                {
                    //Log_Debug("Adding OnListenRequestTimeout, delay %U", request->changeTimeout);
                    onListenRequestTimeout.SetDelay(request->changeTimeout);
                    EventLoop::Add(&onListenRequestTimeout);
                }
                else if (onListenRequestTimeout.GetExpireTime() > EventLoop::Now() + request->changeTimeout)
                {
                    //Log_Debug("Resetting OnListenRequestTimeout, delay %U", request->changeTimeout);
                    EventLoop::Remove(&onListenRequestTimeout);
                    onListenRequestTimeout.SetDelay(request->changeTimeout);
                    EventLoop::Add(&onListenRequestTimeout);
                }
            }
        }
        return;
    }
    
    if (!quorumContext.IsLeader())
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    if (request->type == CLIENTREQUEST_UNREGISTER_SHARDSERVER)
    {
        // make sure it's not part of any quorums
        FOREACH(configQuorum, CONFIG_STATE->quorums)
        {
            if (configQuorum->IsMember(request->nodeID))
            {
                request->response.Failed();
                request->OnComplete();
                return;
            }
        }
    }
    
    if (request->type == CLIENTREQUEST_CREATE_QUORUM)
    {
        // make sure all nodes are currently active
        FOREACH (itNodeID, request->nodes)
        {
            if (!configServer->GetHeartbeatManager()->HasHeartbeat(*itNodeID))
            {
                request->response.Failed();
                request->OnComplete();
                return;
            }
        }
    }

    if (request->type == CLIENTREQUEST_ADD_SHARDSERVER_TO_QUORUM)
    {
        if (!configServer->GetHeartbeatManager()->HasHeartbeat(request->nodeID))
        {
            request->response.Failed();
            request->OnComplete();
            return; 
        }
    }
    
    if (request->type == CLIENTREQUEST_ACTIVATE_SHARDSERVER)
    {
        if (!configServer->GetHeartbeatManager()->HasHeartbeat(request->nodeID))
        {
            request->response.Failed();
            request->OnComplete();
            return; 
        }

        configServer->GetActivationManager()->TryActivateShardServer(request->nodeID);
        request->response.OK();
        request->OnComplete();
        return;
    }

    // TODO: clean up this mess!
    if (request->type == CLIENTREQUEST_MIGRATE_SHARD)
    {
        if (CONFIG_STATE->isMigrating)
            goto MigrationFailed;
        configShard = CONFIG_STATE->GetShard(request->shardID);
        if (!configShard)
            goto MigrationFailed;
        configQuorum = CONFIG_STATE->GetQuorum(configShard->quorumID);
        srcQuorumID = configShard->quorumID;
        if (!configQuorum)
            goto MigrationFailed;
        if (!configQuorum->hasPrimary)
            goto MigrationFailed;
        srcNodeID = configQuorum->primaryID;
        configQuorum = CONFIG_STATE->GetQuorum(request->quorumID);
        if (!configQuorum)
            goto MigrationFailed;
        if (!configQuorum->hasPrimary)
            goto MigrationFailed;
        dstNodeID = configQuorum->primaryID;
        dstQuorumID = request->quorumID;
        if (srcQuorumID == dstQuorumID)
            goto MigrationFailed;

        goto MigrationOk;

        MigrationFailed:
        {
            request->response.Failed();
            request->OnComplete();
            return; 
        }
        MigrationOk:;
    }
    
    configMessage = new ConfigMessage;
    ConstructMessage(request, configMessage);
    
    requests.Append(request);
    configMessages.Append(configMessage);
    TryAppend();
}

void ConfigQuorumProcessor::OnClientClose(ClientSession* session)
{
    ClientRequest*  it;
    ClientRequest*  next;
    
    // TODO: list iteration is not necessary, because it is an InList
    for (it = listenRequests.First(); it != NULL; it = next)
    {
        next = listenRequests.Next(it);
        if (it->session == session)
        {
            listenRequests.Remove(it);
            it->response.NoResponse();
            it->OnComplete();
            break;
        }
    }
}

void ConfigQuorumProcessor::ActivateNode(uint64_t quorumID, uint64_t nodeID, bool force)
{
    ConfigMessage* message;
 
    /* force is not used, it's legacy, should be removed */

    message = new ConfigMessage();
    message->fromClient = false;
    message->ActivateShardServer(quorumID, nodeID, force);
    configMessages.Append(message);
    
    TryAppend();
}

void ConfigQuorumProcessor::DeactivateNode(uint64_t quorumID, uint64_t nodeID, bool force)
{
    ConfigMessage* message;

    message = new ConfigMessage();
    message->fromClient = false;
    message->DeactivateShardServer(quorumID, nodeID, force);
    configMessages.Append(message);
    
    TryAppend();
}

void ConfigQuorumProcessor::TryRegisterShardServer(Endpoint& endpoint)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_REGISTER_SHARDSERVER && it->endpoint == endpoint)
            return;
    }

    msg = new ConfigMessage;
    msg->fromClient = false;
    msg->RegisterShardServer(0, endpoint);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::TryUpdateShardServer(uint64_t nodeID, Endpoint& endpoint)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_REGISTER_SHARDSERVER && it->endpoint == endpoint)
            return;
        if (it->type == CONFIGMESSAGE_REGISTER_SHARDSERVER && it->nodeID == nodeID)
            return;
    }    

    msg = new ConfigMessage;
    msg->fromClient = false;
    msg->RegisterShardServer(nodeID, endpoint);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::TrySplitShardBegin(uint64_t shardID, ReadBuffer splitKey)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_SPLIT_SHARD_BEGIN && it->shardID == shardID)
            return;
    }

    msg = new ConfigMessage;
    msg->fromClient = false;    
    msg->SplitShardBegin(shardID, splitKey);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::TrySplitShardComplete(uint64_t shardID)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_SPLIT_SHARD_COMPLETE && it->shardID == shardID)
            return;
    }

    msg = new ConfigMessage;
    msg->fromClient = false;    
    msg->SplitShardComplete(shardID);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::TryTruncateTableBegin(uint64_t tableID)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN && it->tableID == tableID)
            return;
    }

    msg = new ConfigMessage;
    msg->fromClient = false;    
    msg->TruncateTableBegin(tableID);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::TryTruncateTableComplete(uint64_t tableID)
{
    ConfigMessage*  it;
    ConfigMessage*  msg;

    FOREACH (it, configMessages)
    {
        if (it->type == CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE && it->tableID == tableID)
            return;
    }

    msg = new ConfigMessage;
    msg->fromClient = false;    
    msg->TruncateTableComplete(tableID);

    configMessages.Append(msg);
    TryAppend();
}

void ConfigQuorumProcessor::OnShardMigrationComplete(ClusterMessage& message)
{
    ConfigMessage*  configMessage;
    
    if (!quorumContext.IsLeader() || !CONFIG_STATE->isMigrating)
        return;
    
    configMessage = new ConfigMessage;
    configMessage->fromClient = false;
    configMessage->ShardMigrationComplete(message.quorumID,
     CONFIG_STATE->migrateSrcShardID, CONFIG_STATE->migrateDstShardID);
    configMessages.Append(configMessage);
    TryAppend();
}

void ConfigQuorumProcessor::UpdateListeners(bool force)
{
    ClientRequest*                  itRequest;
    ConfigShardServer*              itShardServer;
    ClusterMessage                  message;
    Buffer                          checksumBuffer;
    bool                            configChanged;
    uint64_t                        now;
    uint32_t                        checksum;

    if (!quorumContext.IsLeader())
        return;

    // check if the configState changed at all
    configChanged = force;
    CONFIG_STATE->Write(checksumBuffer, true);
    checksum = checksumBuffer.GetChecksum();
    if (checksum == 0 || checksum != configStateChecksum)
    {
        configChanged = true;
        configStateChecksum = checksum;
    }

//    if (configChanged)
//        Log_Debug("UpdateListeners: %b, %B", configChanged, &checksumBuffer);

    now = EventLoop::Now();
    
    // update clients
    FOREACH (itRequest, listenRequests)
    {
        if (configChanged ||
         (itRequest->changeTimeout != 0 && 
         itRequest->changeTimeout < now - itRequest->lastChangeTime) ||
         (itRequest->paxosID != 0 && itRequest->paxosID < CONFIG_STATE->paxosID))
        {
//            Log_Debug(
//             "request: %p, changeTimeout: %U, diff: %U, paxosID: %U, configPaxosID: %U",
//             itRequest, itRequest->changeTimeout, now - itRequest->lastChangeTime,
//             itRequest->paxosID, CONFIG_STATE->paxosID);
            itRequest->response.ConfigStateResponse(*CONFIG_STATE);
            itRequest->OnComplete(false);
            itRequest->lastChangeTime = now;
            itRequest->paxosID = CONFIG_STATE->paxosID;
        }
    }
    
    if (!IsMaster())
        return;
    
    if (configChanged || lastConfigChangeTime <= now - 1000)
    {
        lastConfigChangeTime = now;
        
        // update shard servers
        message.SetConfigState(*CONFIG_STATE);
        FOREACH (itShardServer, CONFIG_STATE->shardServers)
        {
            CONTEXT_TRANSPORT->SendClusterMessage(itShardServer->nodeID, message);
        }
    }
}

void ConfigQuorumProcessor::OnLearnLease()
{
    if (!leaseKnown)
    {
        leaseKnown = true;
        leaseOwner = quorumContext.GetLeaseOwner();
        Log_Message("Node %U became the master", leaseOwner);
    }
}

void ConfigQuorumProcessor::OnLeaseTimeout()
{
    ClientRequest*  itRequest;
 
    Log_Message("Node %U is no longer the master", leaseOwner);

    leaseKnown = false;
    leaseOwner = 0;

    // clear config messages   
    configMessages.DeleteList();

    // clear client requests
    FOREACH_FIRST (itRequest, requests)
    {
        requests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }
    ASSERT(requests.GetLength() == 0);
  
    CONFIG_STATE->hasMaster = false;
    CONFIG_STATE->masterID = 0;
    CONFIG_STATE->paxosID = 0;
    configServer->OnConfigStateChanged(); // TODO: is this neccesary?
    // TODO: tell ActivationManager
}

void ConfigQuorumProcessor::OnIsLeader()
{
    bool            updateListeners;
    uint64_t        clusterID;
    ConfigMessage*  configMessage;

    updateListeners = false;
    if (!CONFIG_STATE->hasMaster)
        updateListeners = true;

    CONFIG_STATE->hasMaster = true;
    CONFIG_STATE->masterID = GetMaster();
    CONFIG_STATE->paxosID = GetPaxosID();

    if (updateListeners && (uint64_t) GetMaster() == configServer->GetNodeID())
        UpdateListeners();

    // check cluster ID
    clusterID = REPLICATION_CONFIG->GetClusterID();
    if (clusterID == 0 && configMessages.GetLength() == 0)
    {
        configMessage = new ConfigMessage;
        configMessage->fromClient = false;
        
        clusterID = GenerateGUID();
        configMessage->SetClusterID(clusterID);
        configMessages.Append(configMessage);
        TryAppend();
    }
}

void ConfigQuorumProcessor::OnAppend(uint64_t paxosID, ConfigMessage& message, bool ownAppend)
{
    uint64_t        clusterID;
    ClusterMessage  clusterMessage;
    char            hexbuf[64 + 1];
    bool            success;

    // catchup:
    // if paxosID is smaller or equal to configStatePaxosID, that means
    // our state already includes the writes in this round
    if (paxosID <= configServer->GetDatabaseManager()->GetPaxosID())
        return;

    Log_Trace("paxosID: %U, message.type: %c", paxosID, message.type);

    if (message.type == CONFIGMESSAGE_SHARD_MIGRATION_BEGIN)
    {
        // TODO: error handling
        success = OnShardMigrationBegin(message);
        Log_Debug("shard migration begin, shardID: %U, success: %b", message.shardID, success);
    }

    if (message.type == CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE)
    {
        if (quorumContext.IsLeader())
        {
            if (CONFIG_STATE->isMigrating)
            {
                ASSERT(CONFIG_STATE->migrateQuorumID == message.quorumID);
                ASSERT(CONFIG_STATE->migrateSrcShardID == message.srcShardID);
                ASSERT(CONFIG_STATE->migrateDstShardID == message.dstShardID);
                CONFIG_STATE->isMigrating = false;
                CONFIG_STATE->migrateQuorumID = 0;
                CONFIG_STATE->migrateSrcShardID = 0;
                CONFIG_STATE->migrateDstShardID = 0;
            }
        }
    }
    
    CONFIG_STATE->paxosID = paxosID;
    CONFIG_STATE->OnMessage(message);
    configServer->GetDatabaseManager()->Write();
    configServer->OnConfigStateChanged(); // UpdateActivationTimeout();

    if (message.type == CONFIGMESSAGE_SET_CLUSTER_ID)
    {
        clusterID = REPLICATION_CONFIG->GetClusterID();
        if (clusterID > 0 && message.clusterID != clusterID)
            STOP_FAIL(1, "Invalid controller configuration!");
        
        UInt64ToBufferWithBase(hexbuf, sizeof(hexbuf), message.clusterID, 64);
        Log_Debug("ClusterID set to %s (%U)", hexbuf, message.clusterID);
        CONTEXT_TRANSPORT->SetClusterID(message.clusterID);
        REPLICATION_CONFIG->SetClusterID(message.clusterID);
        REPLICATION_CONFIG->Commit();
    }
    else if (message.type == CONFIGMESSAGE_UNREGISTER_SHARDSERVER)
    {
        // tell shard sherver to stop itself
        clusterMessage.UnregisterStop();
        CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, clusterMessage);
    }    
    if (message.type == CONFIGMESSAGE_REGISTER_SHARDSERVER)
    {
        // tell ContextTransport that this connection has a new nodeID
        CONTEXT_TRANSPORT->SetConnectionNodeID(message.endpoint, message.nodeID);       
        // tell the shard server
        clusterMessage.SetNodeID(REPLICATION_CONFIG->GetClusterID(), message.nodeID);
        CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, clusterMessage);
    }
    else if (message.type == CONFIGMESSAGE_SPLIT_SHARD_BEGIN)
    {
        Log_Message("Split shard process started (parent shardID = %U)...", message.shardID);
    }
    else if (message.type == CONFIGMESSAGE_SPLIT_SHARD_COMPLETE)
    {
        Log_Message("Split shard process completed (new shardID = %U)...", message.shardID);
        CONFIG_STATE->isSplitting = false;
    }
    
    if (IsMaster())
        UpdateListeners();

    if (ownAppend)
    {
        ASSERT(configMessages.GetLength() > 0);
        ASSERT(configMessages.First()->type == message.type);
        if (configMessages.First()->fromClient)
            SendClientResponse(message);
        configMessages.Delete(configMessages.First());
    }
    
    if (configMessages.GetLength() > 0)
        TryAppend();
        
    quorumContext.OnAppendComplete();
}

void ConfigQuorumProcessor::OnStartCatchup()
{
    CatchupMessage msg;
    
    if (isCatchingUp || !quorumContext.IsLeaseKnown())
        return;
    
    quorumContext.StopReplication();
    
    msg.CatchupRequest(configServer->GetNodeID(), quorumContext.GetQuorumID());
    
    CONTEXT_TRANSPORT->SendQuorumMessage(
     quorumContext.GetLeaseOwner(), quorumContext.GetQuorumID(), msg);
     
    isCatchingUp = true;
    
    Log_Message("Catchup started from node %U", quorumContext.GetLeaseOwner());
}

void ConfigQuorumProcessor::OnCatchupMessage(CatchupMessage& imsg)
{
    bool            hasMaster;
    uint64_t        masterID;
    uint64_t        clusterID;
    Buffer          buffer;
    ReadBuffer      keyConfigState;
    ReadBuffer      keyClusterID;
    ReadBuffer      value;
    CatchupMessage  omsg;
    
    keyConfigState.Wrap("ConfigState");
    keyClusterID.Wrap("ClusterID");

    switch (imsg.type)
    {
        case CATCHUPMESSAGE_REQUEST:
            if (!quorumContext.IsLeaseOwner())
                return;
            ASSERT(imsg.quorumID == quorumContext.GetQuorumID());

            // send ConfigState => <ConfigState>
            CONFIG_STATE->Write(buffer);
            value.Wrap(buffer);
            omsg.Set(0, keyConfigState, value); // shardID is unused here
            CONTEXT_TRANSPORT->SendQuorumMessage(imsg.nodeID, quorumContext.GetQuorumID(), omsg);
            
            // send ClusterID => <ClusterID>
            ASSERT(REPLICATION_CONFIG->GetClusterID() > 0);
            buffer.Writef("%U", REPLICATION_CONFIG->GetClusterID());
            value.Wrap(buffer);
            omsg.Set(0, keyClusterID, value); // shardID is unused here
            CONTEXT_TRANSPORT->SendQuorumMessage(imsg.nodeID, quorumContext.GetQuorumID(), omsg);
            
            // send COMMIT with the paxosID whose value is in the db
            omsg.Commit(quorumContext.GetPaxosID() - 1);
            CONTEXT_TRANSPORT->SendQuorumMessage(imsg.nodeID, quorumContext.GetQuorumID(), omsg);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            ASSERT_FAIL();
            break;
        case CATCHUPMESSAGE_SET:
            if (!isCatchingUp)
                return;
            // receive ConfigState => <ConfigState>
            if (ReadBuffer::Cmp(imsg.key, keyConfigState) == 0)
            {
                hasMaster = CONFIG_STATE->hasMaster;
                masterID = CONFIG_STATE->masterID;
            
                if (!CONFIG_STATE->Read(imsg.value))
                    ASSERT_FAIL();
                CONFIG_STATE->hasMaster = hasMaster;
                CONFIG_STATE->masterID = masterID;
            }
            // received ClusterID => <ClusterID>
            else if (ReadBuffer::Cmp(imsg.key, keyClusterID) == 0)
            {
                imsg.value.Readf("%U", &clusterID);
                ASSERT(clusterID > 0);
                CONTEXT_TRANSPORT->SetClusterID(clusterID);
                REPLICATION_CONFIG->SetClusterID(clusterID);
                REPLICATION_CONFIG->Commit();
            }
            break;
        case CATCHUPMESSAGE_COMMIT:
            if (!isCatchingUp)
                return;
            configServer->GetDatabaseManager()->SetPaxosID(imsg.paxosID);
            configServer->GetDatabaseManager()->Write();
            quorumContext.OnCatchupComplete(imsg.paxosID);      // this commits
            isCatchingUp = false;
            quorumContext.ContinueReplication();
            configServer->GetDatabaseManager()->SetControllers();
            Log_Message("Catchup complete");
            break;
        default:
            ASSERT_FAIL();
    }
}

bool ConfigQuorumProcessor::OnShardMigrationBegin(ConfigMessage& message)
{
    uint64_t        srcNodeID, dstNodeID;
    uint64_t        srcQuorumID, dstQuorumID;
    uint64_t        nextShardID;
    ConfigShard*    configShard;
    ConfigQuorum*   configQuorum;
    ClusterMessage  clusterMessage;

    nextShardID = CONFIG_STATE->nextShardID++;

    if (quorumContext.IsLeader())
    {    
        if (CONFIG_STATE->isMigrating)
            return false;
        configShard = CONFIG_STATE->GetShard(message.shardID);
        if (!configShard)
            return false;
        configQuorum = CONFIG_STATE->GetQuorum(configShard->quorumID);
        srcQuorumID = configShard->quorumID;
        if (!configQuorum)
            return false;
        if (!configQuorum->hasPrimary)
            return false;
        srcNodeID = configQuorum->primaryID;
        configQuorum = CONFIG_STATE->GetQuorum(message.quorumID);
        if (!configQuorum)
            return false;
        if (!configQuorum->hasPrimary)
            return false;
        dstNodeID = configQuorum->primaryID;
        dstQuorumID = message.quorumID;
        if (srcQuorumID == dstQuorumID)
            return false;

        CONFIG_STATE->isMigrating = true;
        CONFIG_STATE->migrateQuorumID = message.quorumID;
        CONFIG_STATE->migrateSrcShardID = message.shardID;
        CONFIG_STATE->migrateDstShardID = nextShardID;

        UpdateListeners();

        clusterMessage.ShardMigrationInitiate(dstNodeID,
         CONFIG_STATE->migrateQuorumID,
         CONFIG_STATE->migrateSrcShardID,
         CONFIG_STATE->migrateDstShardID);
        CONTEXT_TRANSPORT->SendClusterMessage(srcNodeID, clusterMessage);
    }
    
    return true;
}

void ConfigQuorumProcessor::ConstructMessage(ClientRequest* request, ConfigMessage* message)
{
    message->fromClient = true;
    
    switch (request->type)
    {
        case CLIENTREQUEST_UNREGISTER_SHARDSERVER:
            message->type = CONFIGMESSAGE_UNREGISTER_SHARDSERVER;
            message->nodeID = request->nodeID;
            return;
        case CLIENTREQUEST_CREATE_QUORUM:
            message->type = CONFIGMESSAGE_CREATE_QUORUM;
            message->name = request->name;
            message->nodes = request->nodes;
            return;
        case CLIENTREQUEST_RENAME_QUORUM:
            message->type = CONFIGMESSAGE_RENAME_QUORUM;
            message->quorumID = request->quorumID;
            message->name = request->name;
            return;
        case CLIENTREQUEST_DELETE_QUORUM:
            message->type = CONFIGMESSAGE_DELETE_QUORUM;
            message->quorumID = request->quorumID;
            return;
        case CLIENTREQUEST_ADD_SHARDSERVER_TO_QUORUM:
            message->type = CONFIGMESSAGE_ADD_SHARDSERVER_TO_QUORUM;
            message->quorumID = request->quorumID;
            message->nodeID = request->nodeID;
            return;
        case CLIENTREQUEST_REMOVE_SHARDSERVER_FROM_QUORUM:
            message->type = CONFIGMESSAGE_REMOVE_SHARDSERVER_FROM_QUORUM;
            message->quorumID = request->quorumID;
            message->nodeID = request->nodeID;
            return;
        case CLIENTREQUEST_SET_PRIORITY:
            message->type = CONFIGMESSAGE_SET_PRIORITY;
            message->quorumID = request->quorumID;
            message->nodeID = request->nodeID;
            message->priority = request->priority;
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
            message->tableID = request->tableID;
            message->name.Wrap(request->name);
            return;
        case CLIENTREQUEST_DELETE_TABLE:
            message->type = CONFIGMESSAGE_DELETE_TABLE;
            message->tableID = request->tableID;
            return;
        case CLIENTREQUEST_TRUNCATE_TABLE:
            message->type = CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN;
            message->tableID = request->tableID;
            return;
        case CLIENTREQUEST_FREEZE_TABLE:
            message->type = CONFIGMESSAGE_FREEZE_TABLE;
            message->tableID = request->tableID;
            return;
        case CLIENTREQUEST_UNFREEZE_TABLE:
            message->type = CONFIGMESSAGE_UNFREEZE_TABLE;
            message->tableID = request->tableID;
            return;
        case CLIENTREQUEST_SPLIT_SHARD:
            message->type = CONFIGMESSAGE_SPLIT_SHARD_BEGIN;
            message->shardID = request->shardID;
            message->splitKey = request->key;
            return;
        case CLIENTREQUEST_MIGRATE_SHARD:
            message->type = CONFIGMESSAGE_SHARD_MIGRATION_BEGIN;
            message->shardID = request->shardID;
            message->quorumID = request->quorumID;
            return;
        default:
            ASSERT_FAIL();
    }
}

void ConfigQuorumProcessor::ConstructResponse(ConfigMessage* message, ClientResponse* response)
{
    switch (response->request->type)
    {
        case CLIENTREQUEST_UNREGISTER_SHARDSERVER:
            response->OK();
            return;
        case CLIENTREQUEST_CREATE_QUORUM:
            response->Number(message->quorumID);
            return;
        case CLIENTREQUEST_RENAME_QUORUM:
            response->OK();
            return;
        case CLIENTREQUEST_DELETE_QUORUM:
            response->OK();
            return;
        case CLIENTREQUEST_ADD_SHARDSERVER_TO_QUORUM:
            response->OK();
            return;
        case CLIENTREQUEST_REMOVE_SHARDSERVER_FROM_QUORUM:
            response->OK();
            return;
        case CLIENTREQUEST_SET_PRIORITY:
            response->OK();
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
        case CLIENTREQUEST_TRUNCATE_TABLE:
            response->OK();
            return;
        case CLIENTREQUEST_FREEZE_TABLE:
            response->OK();
            return;
        case CLIENTREQUEST_UNFREEZE_TABLE:
            response->OK();
            return;
        case CLIENTREQUEST_MIGRATE_SHARD:
            response->OK();
            return;
        case CLIENTREQUEST_SPLIT_SHARD:
            response->Number(message->newShardID);
            return;
        default:
            ASSERT_FAIL();
    }
}

void ConfigQuorumProcessor::SendClientResponse(ConfigMessage& message)
{
    ClientRequest* request;
    
    ASSERT(requests.GetLength() > 0);
    
    request = requests.First();
    requests.Remove(request);
    
    ConstructResponse(&message, &request->response);
    request->OnComplete();
}

// Here we handle HTTP pollConfigState requests with timeout
// To disable this logic, comment the SetCallable line in constructor
void ConfigQuorumProcessor::OnListenRequestTimeout()
{
    ClientRequest*      request;
    uint64_t            now;
    uint64_t            nextTimeout;

    //Log_Debug("OnListenRequestTimeout, numListenRequests: %d", listenRequests.GetLength());

    now = EventLoop::Now();
    nextTimeout = 0;

    FOREACH (request, listenRequests)
    {
        if (request->changeTimeout + request->lastChangeTime <= now)
        {
            request->response.ConfigStateResponse(*CONFIG_STATE);
            request->OnComplete(false);
            request->lastChangeTime = now;
            request->paxosID = CONFIG_STATE->paxosID;
        }
        else if (nextTimeout == 0 || nextTimeout > request->lastChangeTime + request->changeTimeout)
        {
            nextTimeout = request->lastChangeTime + request->changeTimeout;
        }
    }

    if (nextTimeout != 0)
    {
        onListenRequestTimeout.SetDelay(nextTimeout - now);
        EventLoop::Add(&onListenRequestTimeout);
    }
}
