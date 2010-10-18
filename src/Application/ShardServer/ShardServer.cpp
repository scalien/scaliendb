#include "ShardServer.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/CatchupMessage.h"

#define REQUEST_TIMEOUT 1000
#define DATABASE_NAME   "system"

static size_t Hash(uint64_t h)
{
    return h;
}

void ShardServer::Init()
{
    unsigned        numControllers;
    uint64_t        nodeID;
    uint64_t        runID;
    const char*     str;
    Endpoint        endpoint;

    databaseEnv.InitCache(configFile.GetIntValue("database.cacheSize", STORAGE_DEFAULT_CACHE_SIZE));
    databaseEnv.Open(configFile.GetValue("database.dir", "db"));
    systemDatabase = databaseEnv.GetDatabase(DATABASE_NAME);
    REPLICATION_CONFIG->Init(systemDatabase->GetTable("system"));

    runID = REPLICATION_CONFIG->GetRunID();
    runID += 1;
    REPLICATION_CONFIG->SetRunID(runID);
    REPLICATION_CONFIG->Commit();
    Log_Trace("rundID: %" PRIu64, runID);

    if (REPLICATION_CONFIG->GetNodeID() > 0)
    {
        isAwaitingNodeID = false;
        CONTEXT_TRANSPORT->SetSelfNodeID(REPLICATION_CONFIG->GetNodeID());
    }
    else
        isAwaitingNodeID = true;
    
    // connect to the controller nodes
    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
    {
        str = configFile.GetListValue("controllers", nodeID, "");
        endpoint.Set(str);
        CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
        controllers.Append(nodeID);
        // this will cause the node to connect to the controllers
        // and if my nodeID is not set the MASTER will automatically send
        // me a SetNodeID cluster message
    }

    CONTEXT_TRANSPORT->SetClusterContext(this);
    
    isSendingCatchup = false;
    isCatchingUp = false;
    catchupCursor = NULL;
    
    requestTimer.SetCallable(MFUNC(ShardServer, OnRequestLeaseTimeout));
    requestTimer.SetDelay(REQUEST_TIMEOUT);
    
    heartbeatTimeout.SetCallable(MFUNC(ShardServer, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(HEARTBEAT_TIMEOUT);
    EventLoop::Add(&heartbeatTimeout);
}

void ShardServer::Shutdown()
{
    DatabaseMap::Node*  dbNode;
    StorageDatabase*    database;

    CONTEXT_TRANSPORT->Shutdown();
    REPLICATION_CONFIG->Shutdown();
    
    for (dbNode = databases.First(); dbNode != NULL; dbNode = databases.Next(dbNode))
    {
        database = dbNode->Value();
        delete database;
    }

    databaseEnv.Close();
}

ShardServer::QuorumList* ShardServer::GetQuorums()
{
    return &quorums;
}

StorageEnvironment& ShardServer::GetEnvironment()
{
    return databaseEnv;
}

void ShardServer::ProcessMessage(QuorumData* quorumData, DataMessage& message, bool ownAppend)
{
    StorageTable*       table;
    ClientRequest*      request;

    table = LocateTable(message.tableID);
    if (!table)
        ASSERT_FAIL();
    
    request = NULL;
    if (ownAppend)
    {
        assert(quorumData->dataMessages.GetLength() > 0);
        assert(quorumData->dataMessages.First()->type == message.type);
        assert(quorumData->requests.GetLength() > 0);
        request = quorumData->requests.First();
        request->response.OK();
    }
    
    // TODO: differentiate return status (FAILED, NOSERVICE)
    switch (message.type)
    {
        case CLIENTREQUEST_SET:
            if (!table->Set(message.key, message.value))
            {
                if (request)
                    request->response.Failed();
            }
            break;
        case CLIENTREQUEST_DELETE:
            if (!table->Delete(message.key))
            {
                if (request)
                    request->response.Failed();
            }
            break;
        default:
            ASSERT_FAIL();
            break;
    }

    if (ownAppend)
    {
        quorumData->requests.Remove(request);
        request->OnComplete(); // request deletes itself
        quorumData->dataMessages.Delete(quorumData->dataMessages.First());
    }
}

void ShardServer::OnWriteReadyness()
{
    uint64_t            *it;
    CatchupMessage      msg;
    QuorumData*         quorumData;
    StorageShard*       shard;
    StorageKeyValue*    kv;

    kv = catchupCursor->Next();
    if (kv)
    {
        msg.KeyValue(kv->key, kv->value);
        CONTEXT_TRANSPORT->SendQuorumMessage(
         catchupRequest.nodeID, catchupRequest.quorumID, msg);
        return;
    }

    // kv was NULL, at end of current shard
    assert(catchupCursor != NULL);
    delete catchupCursor;

    // find next shard
    quorumData = LocateQuorum(catchupRequest.quorumID);
    if (!quorumData)
        ASSERT_FAIL();

    // iterate to current, which is catchupShardID
    FOREACH(it, quorumData->shards)
    {
        if (*it == catchupShardID)
            break;
    }
    assert(it != NULL);
    // move to next
    it = quorumData->shards.Next(it);

    if (it)
    {
        // there is a next shard, start sending it now
        catchupShardID = *it;
        shard = LocateShard(catchupShardID);
        assert(shard != NULL);
        catchupCursor = new StorageCursor(shard);
        msg.BeginShard(catchupShardID);
        CONTEXT_TRANSPORT->SendQuorumMessage(
        catchupRequest.nodeID, catchupRequest.quorumID, msg);
        // send first KV
        kv = catchupCursor->Begin(ReadBuffer());
        if (kv)
        {
            msg.KeyValue(kv->key, kv->value);
            CONTEXT_TRANSPORT->SendQuorumMessage(
             catchupRequest.nodeID, catchupRequest.quorumID, msg);
        }
    }
    else
    {
        // there is no next shard, we're all done, send commit
        msg.Commit(catchupPaxosID);
        // send the paxosID whose value is in the db
        CONTEXT_TRANSPORT->SendQuorumMessage(
         catchupRequest.nodeID, catchupRequest.quorumID, msg);
        
        CONTEXT_TRANSPORT->UnregisterWriteReadyness(
         catchupRequest.nodeID, MFUNC(ShardServer, OnWriteReadyness));
    }    
}

bool ShardServer::IsLeaseKnown(uint64_t quorumID)
{
    QuorumData*     quorumData;
    ConfigQuorum*   configQuorum;
    
    quorumData = LocateQuorum(quorumID);
    if (quorumData == NULL)
        return false;

    if (quorumData->isPrimary)
        return true;
    
    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return false;
    
    if (!configQuorum->hasPrimary)
        return false;
    
    // we already checked this case in quorumData earlier
    if (configQuorum->primaryID == REPLICATION_CONFIG->GetNodeID())
        return false;
    
    return true;
}

bool ShardServer::IsLeaseOwner(uint64_t quorumID)
{
    QuorumData*     quorumData;
    
    quorumData = LocateQuorum(quorumID);
    if (quorumData == NULL)
        return false;
     
    return quorumData->isPrimary;
}

uint64_t ShardServer::GetLeaseOwner(uint64_t quorumID)
{
    QuorumData*     quorumData;
    ConfigQuorum*   configQuorum;
    
    quorumData = LocateQuorum(quorumID);
    if (quorumData == NULL)
        return 0;

    if (quorumData->isPrimary)
        return REPLICATION_CONFIG->GetNodeID();
    
    configQuorum = configState.GetQuorum(quorumID);
    if (configQuorum == NULL)
        return 0;
    
    if (!configQuorum->hasPrimary)
        return 0;
    
    // we already checked this case in quorumData earlier
    if (configQuorum->primaryID == REPLICATION_CONFIG->GetNodeID())
        return 0;
    
    return configQuorum->primaryID;
}

void ShardServer::OnAppend(uint64_t quorumID, ReadBuffer& value, bool ownAppend)
{
    int                 read;
    QuorumData*         quorumData;
    DataMessage         message;
    
    Log_Trace();
    
    quorumData = LocateQuorum(quorumID);
    if (!quorumData)
        ASSERT_FAIL();
        
    while (value.GetLength() > 0)
    {
        read = message.Read(value);
        assert(read > 0);
        value.Advance(read);

        ProcessMessage(quorumData, message, ownAppend);
    }
    
    if (quorumData->dataMessages.GetLength() > 0)
        TryAppend(quorumData);
}

void ShardServer::OnStartCatchup(uint64_t quorumID)
{
    QuorumData*     quorumData;
    CatchupMessage  msg;

    quorumData = LocateQuorum(quorumID);
    if (!quorumData)
        ASSERT_FAIL();

    if (isCatchingUp || !quorumData->context.IsLeaseKnown())
        return;
    
    quorumData->context.StopReplication();
    
    msg.CatchupRequest(REPLICATION_CONFIG->GetNodeID(), quorumID);
    
    CONTEXT_TRANSPORT->SendQuorumMessage(
     quorumData->context.GetLeaseOwner(), quorumID, msg);
     
    isCatchingUp = true;
    catchupQuorum = quorumData;
    
    Log_Message("Catchup started from node %" PRIu64 "", quorumData->context.GetLeaseOwner());
}

void ShardServer::OnCatchupMessage(CatchupMessage& imsg)
{
    switch (imsg.type)
    {
        case CATCHUPMESSAGE_REQUEST:
            OnCatchupRequest(imsg);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            OnCatchupBeginShard(imsg);
            break;
        case CATCHUPMESSAGE_KEYVALUE:
            OnCatchupKeyValue(imsg);
            break;
        case CATCHUPMESSAGE_COMMIT:
            OnCatchupCommit(imsg);
            break;
        default:
            ASSERT_FAIL();
    }
}

bool ShardServer::IsValidClientRequest(ClientRequest* request)
{
     return request->IsShardServerRequest();
}

void ShardServer::OnClientRequest(ClientRequest* request)
{
    DataMessage*    message;
    ConfigShard*    shard;
    QuorumData*     quorumData;
    
    shard = configState.GetShard(request->tableID, ReadBuffer(request->key));
    if (!shard)
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }

    quorumData = LocateQuorum(shard->quorumID);
    if (!quorumData)
    {
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    if (!quorumData->context.IsLeader())
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }

    if (request->type == CLIENTREQUEST_GET)
        return OnClientRequestGet(request);

    message = new DataMessage;
    FromClientRequest(request, message);
    
    Log_Trace("message.type = %c, message.key = %.*s", message->type, P(&message->key));
    
    quorumData->requests.Append(request);
    quorumData->dataMessages.Append(message);
    TryAppend(quorumData);
}

void ShardServer::OnClientClose(ClientSession* /*session*/)
{
    // nothing
}

void ShardServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& msg)
{
    Log_Trace();
    
    switch (msg.type)
    {
        case CLUSTERMESSAGE_SET_NODEID:
            if (!isAwaitingNodeID)
                return;
            isAwaitingNodeID = false;
            CONTEXT_TRANSPORT->SetSelfNodeID(msg.nodeID);
            REPLICATION_CONFIG->SetNodeID(msg.nodeID);
            REPLICATION_CONFIG->Commit();
            Log_Trace("my nodeID is %" PRIu64 "", msg.nodeID);
            break;
        case CLUSTERMESSAGE_SET_CONFIG_STATE:
            OnSetConfigState(msg.configState);
            Log_Trace("got new configState, master is %d", 
             configState.hasMaster ? (int) configState.masterID : -1);
            break;
        case CLUSTERMESSAGE_RECEIVE_LEASE:
            OnReceiveLease(msg);
            Log_Trace("recieved lease, quorumID = %" PRIu64 ", proposalID =  %" PRIu64,
             msg.quorumID, msg.proposalID);
            break;
        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
    // nothing
}

bool ShardServer::OnAwaitingNodeID(Endpoint /*endpoint*/)
{
    // always drop
    return true;
}

QuorumData* ShardServer::LocateQuorum(uint64_t quorumID)
{
    QuorumData* quorum;
    
    for (quorum = quorums.First(); quorum != NULL; quorum = quorums.Next(quorum))
    {
        if (quorum->quorumID == quorumID)
            return quorum;
    }
    
    return NULL;
}

// TODO: this should be moved to QuorumData
void ShardServer::TryAppend(QuorumData* quorumData)
{
    Buffer          singleBuffer;
    DataMessage*    it;
    
    assert(quorumData->dataMessages.GetLength() > 0);
    
    if (!quorumData->context.IsAppending() && quorumData->dataMessages.GetLength() > 0)
    {
        Buffer& value = quorumData->context.GetNextValue();
        FOREACH(it, quorumData->dataMessages)
        {
            singleBuffer.Clear();
            it->Write(singleBuffer);
            if (value.GetLength() + 1 + singleBuffer.GetLength() < DATABASE_REPLICATION_SIZE)
                value.Appendf("%B", &singleBuffer);
            else
                break;
        }
        
        assert(value.GetLength() > 0);
        
        quorumData->context.Append();
    }
}

void ShardServer::FromClientRequest(ClientRequest* request, DataMessage* message)
{
    switch (request->type)
    {
        case CLIENTREQUEST_SET:
            message->type = DATAMESSAGE_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            return;
        case CLIENTREQUEST_DELETE:
            message->type = DATAMESSAGE_DELETE;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            return;
        default:
            ASSERT_FAIL();
    }
}

void ShardServer::OnRequestLeaseTimeout()
{
    QuorumData*     quorum;
    ClusterMessage  msg;
    uint64_t        nodeID;
    uint64_t        *nit;
    
    Log_Trace();
    
    nodeID = REPLICATION_CONFIG->GetNodeID();
    for (quorum = quorums.First(); quorum != NULL; quorum = quorums.Next(quorum))
    {
        quorum->proposalID = REPLICATION_CONFIG->NextProposalID(quorum->proposalID);
        msg.RequestLease(nodeID, quorum->quorumID, quorum->proposalID,
         quorum->context.GetPaxosID(), quorum->configID, PAXOSLEASE_MAX_LEASE_TIME);
        
        // send to all controllers
        for (nit = controllers.First(); nit != NULL; nit = controllers.Next(nit))
            CONTEXT_TRANSPORT->SendClusterMessage(*nit, msg);

        quorum->requestedLeaseExpireTime = EventLoop::Now() + PAXOSLEASE_MAX_LEASE_TIME;
    }
    
    if (!requestTimer.IsActive())
        EventLoop::Add(&requestTimer);
}

void ShardServer::OnPrimaryLeaseTimeout()
{
    uint64_t        now;
    QuorumData*     quorum;
    
    now = EventLoop::Now();

    for (quorum = quorums.First(); quorum != NULL; quorum = quorums.Next(quorum))
    {
        if (now < quorum->leaseTimeout.GetExpireTime())
            continue;

        quorum->isPrimary = false;
        EventLoop::Remove(&quorum->leaseTimeout);
        quorum->context.OnLeaseTimeout();
    }
}

void ShardServer::OnHeartbeatTimeout()
{
    unsigned                numControllers;
    uint64_t                nodeID;
    ClusterMessage          msg;
    QuorumData*             it;
    QuorumPaxosID           quorumPaxosID;
    QuorumPaxosID::List     quorumPaxosIDList;
    
    Log_Trace();
    
    EventLoop::Add(&heartbeatTimeout);
    
    if (CONTEXT_TRANSPORT->IsAwaitingNodeID())
    {
        Log_Trace("not sending heartbeat");
        return;
    }
    FOREACH(it, quorums)
    {
        quorumPaxosID.quorumID = it->quorumID;
        quorumPaxosID.paxosID = it->context.GetPaxosID();
        quorumPaxosIDList.Append(quorumPaxosID);
    }
    
    msg.Heartbeat(CONTEXT_TRANSPORT->GetSelfNodeID(), quorumPaxosIDList);

    numControllers = (unsigned) configFile.GetListNum("controllers");
    for (nodeID = 0; nodeID < numControllers; nodeID++)
        CONTEXT_TRANSPORT->SendClusterMessage(nodeID, msg);
}

void ShardServer::OnSetConfigState(ConfigState& configState_)
{
    QuorumData*             quorum;
    QuorumData*             next;
    ConfigQuorum*           configQuorum;
    ConfigQuorum::NodeList* nodes;
    uint64_t*               nit;
    uint64_t                nodeID;
    bool                    found;

    Log_Trace();

    configState = configState_;
    
    nodeID = REPLICATION_CONFIG->GetNodeID();

    // look for removal
    for (quorum = quorums.First(); quorum != NULL; quorum = next)
    {
        configQuorum = configState.GetQuorum(quorum->quorumID);
        if (configQuorum == NULL)
        {
            // TODO: quorum has disappeared?
            ASSERT_FAIL();
        }
        
        found = false;
        next = quorums.Next(quorum);
        
        nodes = &configQuorum->activeNodes;
        for (nit = nodes->First(); nit != NULL; nit = nodes->Next(nit))
        {
            if (*nit == nodeID)
            {
                found = true;
                break;
            }
        }
        if (found)
            continue;

        nodes = &configQuorum->inactiveNodes;
        for (nit = nodes->First(); nit != NULL; nit = nodes->Next(nit))
        {
            if (*nit == nodeID)
            {
                found = true;
                break;
            }
        }
        if (found)
            continue;
        
        next = quorums.Remove(quorum);
        delete quorum;
    }

    // check changes in active or inactive node list
    FOREACH(configQuorum, configState.quorums)
    {
        nodes = &configQuorum->activeNodes;
        FOREACH(nit, *nodes)
        {
            if (*nit == nodeID)
            {
                ConfigureQuorum(configQuorum, true);
                break;
            }
        }

        nodes = &configQuorum->inactiveNodes;
        FOREACH(nit, *nodes)
        {
            if (*nit == nodeID)
            {
                ConfigureQuorum(configQuorum, false);
                TryReplicationCatchup(configQuorum);
                break;
            }
        }
    }
    
    OnRequestLeaseTimeout();
}

void ShardServer::ConfigureQuorum(ConfigQuorum* configQuorum, bool active)
{
    QuorumData*         quorumData;
    ConfigShardServer*  shardServer;
    uint64_t*           nit;
    uint64_t            quorumID;
    uint64_t            nodeID;
    
    Log_Trace();    
    
    nodeID = REPLICATION_CONFIG->GetNodeID();
    quorumID = configQuorum->quorumID;
    
    quorumData = LocateQuorum(quorumID);
    if (active && quorumData == NULL)
    {
        quorumData = new QuorumData;
        quorumData->quorumID = quorumID;
        quorumData->proposalID = 0;
        quorumData->context.Init(this, configQuorum, GetQuorumTable(quorumID));
        quorumData->leaseTimeout.SetCallable(MFUNC(ShardServer, OnPrimaryLeaseTimeout));
        quorumData->isPrimary = false;
        quorumData->isActive = true;

        quorums.Append(quorumData);
        for (nit = configQuorum->activeNodes.First(); nit != NULL; nit = configQuorum->activeNodes.Next(nit))
        {
            shardServer = configState.GetShardServer(*nit);
            assert(shardServer != NULL);
            CONTEXT_TRANSPORT->AddNode(*nit, shardServer->endpoint);
        }
        CONTEXT_TRANSPORT->AddQuorumContext(&quorumData->context);
    }
    else if (quorumData != NULL)
    {
        quorumData->context.UpdateConfig(configQuorum->activeNodes);

        // add nodes to CONTEXT_TRANSPORT        
        for (nit = configQuorum->activeNodes.First(); nit != NULL; nit = configQuorum->activeNodes.Next(nit))
        {
            shardServer = configState.GetShardServer(*nit);
            assert(shardServer != NULL);
            CONTEXT_TRANSPORT->AddNode(*nit, shardServer->endpoint);
        }
    }
    
    UpdateStorageShards(configQuorum->shards);
}

void ShardServer::TryReplicationCatchup(ConfigQuorum* configQuorum)
{
    QuorumData*     quorumData;

    quorumData = LocateQuorum(configQuorum->quorumID);
    if (!quorumData)
        return;
    
    // TODO: xxx
}

void ShardServer::OnReceiveLease(ClusterMessage& msg)
{
    QuorumData*     quorum;
    
    quorum = LocateQuorum(msg.quorumID);
    if (!quorum)
        return;
    
    if (quorum->proposalID != msg.proposalID)
        return;
    
    quorum->configID = msg.configID;
    quorum->activeNodes = msg.activeNodes;
    quorum->context.UpdateConfig(quorum->activeNodes);
    quorum->isPrimary = true;
    quorum->leaseTimeout.SetExpireTime(quorum->requestedLeaseExpireTime);
    EventLoop::Reset(&quorum->leaseTimeout);
    
    quorum->context.OnLearnLease();
}

void ShardServer::UpdateStorageShards(List<uint64_t>& shards)
{
    uint64_t*           sit;
    ConfigShard*        shard;
    StorageDatabase*    database;
    StorageTable*       table;
    Buffer              name;
    ReadBuffer          firstKey;

    for (sit = shards.First(); sit != NULL; sit = shards.Next(sit))
    {
        shard = configState.GetShard(*sit);
        assert(shard != NULL);
        
        if (!databases.Get(shard->databaseID, database))
        {
            name.Writef("%U", shard->databaseID);
            name.NullTerminate();

            database = databaseEnv.GetDatabase(name.GetBuffer());
            databases.Set(shard->databaseID, database);
        }
        
        if (!tables.Get(shard->tableID, table))
        {
            name.Writef("%U", shard->tableID);
            name.NullTerminate();
            
            table = database->GetTable(name.GetBuffer());
            assert(table != NULL);
            tables.Set(shard->tableID, table);
        }
        
        // check if key already exists
        firstKey.Wrap(shard->firstKey);
        if (!table->ShardExists(firstKey))
            table->CreateShard(*sit, firstKey);
    }
}

StorageTable* ShardServer::LocateTable(uint64_t tableID)
{
    StorageTable*   table;
    
    if (!tables.Get(tableID, table))
        return NULL;
    return table;
}

StorageShard* ShardServer::LocateShard(uint64_t shardID)
{
    ConfigShard*    shard;
    StorageTable*   table;
    
    shard = configState.GetShard(shardID);
    if (!shard)
        return NULL;
    
    table = LocateTable(shard->tableID);
    if (!table)
        return NULL;
    
    return table->GetShard(shardID);
}

StorageTable* ShardServer::GetQuorumTable(uint64_t quorumID)
{
    Buffer          qname;
    
    qname.Writef("%U", quorumID);
    qname.NullTerminate();
    return systemDatabase->GetTable(qname.GetBuffer());
}

void ShardServer::OnClientRequestGet(ClientRequest* request)
{
    StorageTable*   table;
    ReadBuffer      key;
    ReadBuffer      value;

    table = LocateTable(request->tableID);
    if (!table)
    {
        request->response.Failed();
        request->OnComplete();
        return;            
    }
    
    key.Wrap(request->key);
    if (!table->Get(key, value))
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }
    
    request->response.Value(value);
    request->OnComplete();
}

void ShardServer::OnCatchupRequest(CatchupMessage& msg)
{
    CatchupMessage      omsg;
    Buffer              buffer;
    ReadBuffer          key;
    ReadBuffer          value;
    QuorumData*         quorumData;
    StorageShard*       shard;
    StorageKeyValue*    kv;
    
    if (isSendingCatchup)
        return;

    catchupRequest = msg;
    quorumData = LocateQuorum(catchupRequest.quorumID);
    if (!quorumData)
        ASSERT_FAIL();

    if (!quorumData->context.IsLeader())
        return;

    catchupPaxosID = quorumData->context.GetPaxosID() - 1;
    if (quorumData->shards.GetLength() == 0)
    {
        // no shards in quorums, send Commit
        omsg.Commit(catchupPaxosID);
        // send the paxosID whose value is in the db
        CONTEXT_TRANSPORT->SendQuorumMessage(
         catchupRequest.nodeID, catchupRequest.quorumID, omsg);
        return;
    }
    
    // use the WriteReadyness mechanism to send the KVs
    // OnWriteReadyness() will be called when the next KVs can be sent
    CONTEXT_TRANSPORT->RegisterWriteReadyness(
     catchupRequest.nodeID, MFUNC(ShardServer, OnWriteReadyness));

    catchupShardID = *quorumData->shards.First();
    shard = LocateShard(catchupShardID);
    assert(shard != NULL);
    catchupCursor = new StorageCursor(shard);
    omsg.BeginShard(catchupShardID);
    CONTEXT_TRANSPORT->SendQuorumMessage(
     catchupRequest.nodeID, catchupRequest.quorumID, omsg);

    // send first KV
    kv = catchupCursor->Begin(ReadBuffer());
    if (kv)
    {
        omsg.KeyValue(kv->key, kv->value);
        CONTEXT_TRANSPORT->SendQuorumMessage(
         catchupRequest.nodeID, catchupRequest.quorumID, omsg);
    }
    isSendingCatchup = true;
}

void ShardServer::OnCatchupBeginShard(CatchupMessage& msg)
{
    ConfigShard*    cshard;
    StorageTable*   table;
    ReadBuffer      firstKey;

    if (!isCatchingUp)
        return;

    cshard = configState.GetShard(msg.shardID);
    if (!cshard)
    {
        ASSERT_FAIL();
        return;
    }
        
    table = LocateTable(cshard->tableID);
    if (!table)
    {
        ASSERT_FAIL();
        return;
    }
    
    catchupShard = table->GetShard(msg.shardID);
    if (catchupShard != NULL)
        table->DeleteShard(msg.shardID);
    
    // shard is either deleted or not yet created, therefore create it on disk
    firstKey.Wrap(cshard->firstKey);
    table->CreateShard(msg.shardID, firstKey);
    catchupShard = table->GetShard(msg.shardID);
}

void ShardServer::OnCatchupKeyValue(CatchupMessage& msg)
{
    if (!isCatchingUp)
        return;
    
    assert(catchupShard != NULL);
    catchupShard->Set(msg.key, msg.value, true);
}

void ShardServer::OnCatchupCommit(CatchupMessage& msg)
{
    if (!isCatchingUp)
        return;

    assert(catchupShard != NULL);
    assert(catchupQuorum != NULL);

    catchupQuorum->context.OnCatchupComplete(msg.paxosID);      // this commits
    catchupQuorum->context.ContinueReplication();
    
    catchupQuorum = NULL;
    catchupShard = NULL;
    isCatchingUp = false;

    Log_Message("Catchup complete");
}

QuorumData::QuorumData()
{
    prev = next = this;
    isPrimary = false;
    requestedLeaseExpireTime = 0;
    configID = 0;
}

