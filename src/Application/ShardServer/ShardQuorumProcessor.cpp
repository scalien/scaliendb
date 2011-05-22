#include "ShardQuorumProcessor.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"

#define SHARD_MIGRATION_WRITER (shardServer->GetShardMigrationWriter())

static bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

ShardLeaseRequest::ShardLeaseRequest()
{
    prev = next = this;
}

void ShardAppendState::Reset()
{
    currentAppend = false;
    paxosID = 0;
    commandID = 0;
    valueBuffer.Reset();
    value.Reset();
}

ShardQuorumProcessor::ShardQuorumProcessor()
{
    prev = next = this;

    requestLeaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnRequestLeaseTimeout));
    requestLeaseTimeout.SetDelay(NORMAL_PRIMARYLEASE_REQUEST_TIMEOUT);
    
    activationTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnActivationTimeout));
    activationTimeout.SetDelay(ACTIVATION_TIMEOUT);

    unblockShardTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnUnblockShardTimeout));
    unblockShardTimeout.SetDelay(UNBLOCK_SHARD_TIMEOUT);

    leaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnLeaseTimeout));
    tryAppend.SetCallable(MFUNC(ShardQuorumProcessor, TryAppend));
    resumeAppend.SetCallable(MFUNC(ShardQuorumProcessor, OnResumeAppend));
    localExecute.SetCallable(MFUNC(ShardQuorumProcessor, LocalExecute));
}

void ShardQuorumProcessor::Init(ConfigQuorum* configQuorum, ShardServer* shardServer_)
{
    shardServer = shardServer_;
    catchupWriter.Init(this);
    catchupReader.Init(this);
    isPrimary = false;
    highestProposalID = 0;
    configID = 0;
    migrateShardID = 0;
    migrateNodeID = 0;
    migrateCache = 0;
    blockedShardID = 0;
    appendState.Reset();
    quorumContext.Init(configQuorum, this);
    CONTEXT_TRANSPORT->AddQuorumContext(&quorumContext);
    EventLoop::Add(&requestLeaseTimeout);
}

void ShardQuorumProcessor::Shutdown()
{
    ShardMessage*       itMessage;
    
    leaseRequests.DeleteList();
  
    FOREACH(itMessage, shardMessages)
    {
        if (!itMessage->clientRequest)
            continue;
        
        itMessage->clientRequest->response.NoService();
        itMessage->clientRequest->OnComplete();
        itMessage->clientRequest = NULL;
    }
    shardMessages.DeleteList();
    
    if (requestLeaseTimeout.IsActive())
        EventLoop::Remove(&requestLeaseTimeout);

    if (activationTimeout.IsActive())
        EventLoop::Remove(&activationTimeout);

    if (leaseTimeout.IsActive())
        EventLoop::Remove(&leaseTimeout);

    if (tryAppend.IsActive())
        EventLoop::Remove(&tryAppend);

    if (resumeAppend.IsActive())
        EventLoop::Remove(&resumeAppend);

    if (localExecute.IsActive())
        EventLoop::Remove(&localExecute);
    
    if (catchupReader.IsActive())
        catchupReader.Abort();
    if (catchupWriter.IsActive())
        catchupWriter.Abort();
    
    CONTEXT_TRANSPORT->RemoveQuorumContext(&quorumContext);
    quorumContext.Shutdown();
}

ShardServer* ShardQuorumProcessor::GetShardServer()
{
    return shardServer;
}

bool ShardQuorumProcessor::IsPrimary()
{
    return isPrimary;
}

uint64_t ShardQuorumProcessor::GetQuorumID()
{
    return quorumContext.GetQuorumID();
}

uint64_t ShardQuorumProcessor::GetPaxosID()
{
    return quorumContext.GetPaxosID();
}

void ShardQuorumProcessor::SetPaxosID(uint64_t paxosID)
{
    quorumContext.SetPaxosID(paxosID);
}

ConfigQuorum* ShardQuorumProcessor::GetConfigQuorum()
{
    return shardServer->GetConfigState()->GetQuorum(GetQuorumID());
}

void ShardQuorumProcessor::OnReceiveLease(ClusterMessage& message)
{
    bool                    restartReplication;
    uint64_t*               it;
    ShardLeaseRequest*      lease;
    ShardLeaseRequest*      itLease;
    
//    Log_Debug("Received lease for quorum %U with proposalID %U", GetQuorumID(), message.proposalID);

    FOREACH (itLease, leaseRequests)
        if (itLease->proposalID == message.proposalID)
            break;

    if (!itLease)
    {
        Log_Debug("Dropping received lease because proposalID not found in request list");
        return;
    }
    
    lease = itLease; // found

    // go through lease list and remove leases before the current
    FOREACH_FIRST (itLease, leaseRequests)
    {
        if (itLease == lease)
            break;
        leaseRequests.Delete(itLease);
    }
    
    if (lease->expireTime < EventLoop::Now())
    {
        leaseRequests.Delete(lease);
        Log_Debug("Dropping received lease because it has expired");
        return;
    }

    SortedList<uint64_t>& shards = shardServer->GetConfigState()->GetQuorum(GetQuorumID())->shards;
    if (shards != message.shards)
        return;
    
    if (!isPrimary)
        Log_Debug("Primary for quorum %U", quorumContext.GetQuorumID());
    
    isPrimary = true;
    configID = message.configID;
    
    if (message.activeNodes.GetLength() != activeNodes.GetLength())
        restartReplication = true;
    else
        restartReplication = false;
    
    activeNodes.Clear();
    FOREACH (it, message.activeNodes)
        activeNodes.Add(*it);
//    quorumContext.SetActiveNodes(activeNodes);
        
    shards = message.shards;
    
    leaseTimeout.SetExpireTime(lease->expireTime);
    EventLoop::Reset(&leaseTimeout);
    
    quorumContext.OnLearnLease();
    
    // shard migration
    if (message.watchingPaxosID)
        quorumContext.AppendDummy();
    
    leaseRequests.Delete(lease);
    
    if (restartReplication)
        quorumContext.RestartReplication();
}

void ShardQuorumProcessor::OnStartProposing()
{
    quorumContext.SetQuorumNodes(activeNodes);
}

void ShardQuorumProcessor::OnAppend(uint64_t paxosID, Buffer& value, bool ownAppend)
{
    appendState.paxosID = paxosID;
    appendState.commandID = 0;
    appendState.valueBuffer.Write(value);
    appendState.value.Wrap(appendState.valueBuffer);
    appendState.currentAppend = ownAppend && quorumContext.IsLeaseOwner();

    OnResumeAppend();
}

void ShardQuorumProcessor::OnStartCatchup()
{
    CatchupMessage  msg;

    if (catchupReader.IsActive() || !quorumContext.IsLeaseKnown())
        return;
    
    quorumContext.StopReplication();
    quorumContext.ResetReplicationState();
    
    msg.CatchupRequest(MY_NODEID, quorumContext.GetQuorumID());
    
    CONTEXT_TRANSPORT->SendQuorumMessage(
     quorumContext.GetLeaseOwner(), quorumContext.GetQuorumID(), msg);
     
    catchupReader.Begin();
    
    Log_Message("Catchup started from node %U", quorumContext.GetLeaseOwner());
}

void ShardQuorumProcessor::OnCatchupMessage(CatchupMessage& message)
{
    switch (message.type)
    {
        case CATCHUPMESSAGE_REQUEST:
            if (!catchupWriter.IsActive())
                catchupWriter.Begin(message);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            if (catchupReader.IsActive())
                catchupReader.OnBeginShard(message);
            break;
        case CATCHUPMESSAGE_SET:
            if (catchupReader.IsActive())
                catchupReader.OnSet(message);
            break;
        case CATCHUPMESSAGE_DELETE:
            if (catchupReader.IsActive())
                catchupReader.OnDelete(message);
            break;
        case CATCHUPMESSAGE_COMMIT:
            if (catchupReader.IsActive())
            {
                catchupReader.OnCommit(message);
                quorumContext.OnCatchupComplete(message.paxosID); // this commits
                quorumContext.ContinueReplication();
            }
            break;
        case CATCHUPMESSAGE_ABORT:
            if (catchupReader.IsActive())
            {
                catchupReader.OnAbort(message);
                quorumContext.ContinueReplication();
            }
            break;
        default:
            ASSERT_FAIL();
    }
}

bool ShardQuorumProcessor::IsPaxosBlocked()
{
    return resumeAppend.IsActive();
}

void ShardQuorumProcessor::OnRequestLeaseTimeout()
{
    uint64_t            expireTime;
    ShardLeaseRequest*  lease;
    ClusterMessage      msg;
    
    Log_Trace();
    
    highestProposalID = REPLICATION_CONFIG->NextProposalID(highestProposalID);
    msg.RequestLease(MY_NODEID, quorumContext.GetQuorumID(), highestProposalID,
     GetPaxosID(), configID, PAXOSLEASE_MAX_LEASE_TIME);
    
//    Log_Debug("Requesting lease for qu    orum %U with proposalID %U", GetQuorumID(), highestProposalID);
    
    shardServer->BroadcastToControllers(msg);

    expireTime = EventLoop::Now() + PAXOSLEASE_MAX_LEASE_TIME;
    if (!requestLeaseTimeout.IsActive())
        EventLoop::Add(&requestLeaseTimeout);

    lease = new ShardLeaseRequest;
    lease->proposalID = highestProposalID;
    lease->expireTime = expireTime;
    leaseRequests.Append(lease);
    
    while (leaseRequests.GetLength() > MAX_LEASE_REQUESTS)
        leaseRequests.Delete(leaseRequests.First());
}

void ShardQuorumProcessor::OnLeaseTimeout()
{
    ShardMessage*   itMessage, *nextMessage;
    
    quorumContext.OnLeaseTimeout();
    
    Log_Debug("Lease lost for quorum %U, dropping pending requests...", GetQuorumID());
    
    for (itMessage = shardMessages.First(); itMessage != NULL; itMessage = nextMessage)
    {
        nextMessage = shardMessages.Next(itMessage);
        
        if (itMessage->isBulk)
            continue;

        if (itMessage->clientRequest)
        {
            itMessage->clientRequest->response.NoService();
            itMessage->clientRequest->OnComplete();
            itMessage->clientRequest = NULL;
        }
        shardMessages.Remove(itMessage);
        delete itMessage;
    }
    
    appendState.currentAppend = false;
    isPrimary = false;
    migrateShardID = 0;
    migrateNodeID = 0;
    migrateCache = 0;
    blockedShardID = 0;
    
    if (catchupWriter.IsActive())
        catchupWriter.Abort();
}

void ShardQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ShardMessage*   message;
    ConfigQuorum*   configQuorum;

    configQuorum = shardServer->GetConfigState()->GetQuorum(GetQuorumID());
    ASSERT(configQuorum);

    // strictly consistent messages can only be served by the leader
    if (request->paxosID == 1 && !quorumContext.IsLeader() && !request->isBulk)
    {
        Log_Trace();
        request->response.NoService();
        request->OnComplete();
        return;
    }
    
    // read-your-write consistency
    if (request->paxosID > quorumContext.GetPaxosID())
    {
        Log_Trace();
        request->response.NoService();
        request->OnComplete();
        return;
    }

    if (request->type == CLIENTREQUEST_GET)
    {
        shardServer->GetDatabaseManager()->OnClientReadRequest(request);        
        return;
    }
    
    if (request->IsList())
    {
        shardServer->GetDatabaseManager()->OnClientListRequest(request);
        return;
    }
        
    if (request->type == CLIENTREQUEST_SUBMIT && quorumContext.GetQuorum()->GetNumNodes() > 1)
    {
        if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
            EventLoop::Add(&tryAppend);
        request->response.NoResponse();
        request->OnComplete();
        return;
    }
    
    message = new ShardMessage;
    TransformRequest(request, message);
    
    Log_Trace("message.type = %c, message.key = %R", message->type, &message->key);

    message->clientRequest = request;
    shardMessages.Append(message);

//    if (request->isBulk ||
//     (configQuorum->activeNodes.GetLength() == 1 && configQuorum->inactiveNodes.GetLength() == 0))
//    {
//        if (!localExecute.IsActive())
//            EventLoop::Add(&localExecute);
//        return;
//    }
    
    if (!tryAppend.IsActive())
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::OnClientClose(ClientSession* /*session*/)
{
}

//void ShardQuorumProcessor::SetActiveNodes(SortedList<uint64_t>& activeNodes_)
//{
//    uint64_t* it;
//    
//    activeNodes.Clear();
//    FOREACH(it, activeNodes_)
//        activeNodes.Add(*it);
//
//    quorumContext.SetActiveNodes(activeNodes);
//}

void ShardQuorumProcessor::RegisterPaxosID(uint64_t paxosID)
{
    quorumContext.RegisterPaxosID(paxosID);
}

void ShardQuorumProcessor::TryReplicationCatchup()
{
    // this is called if we're an inactive node and we should probably try to catchup
    
    if (catchupReader.IsActive())
        return;
    
    quorumContext.TryReplicationCatchup();
}

uint64_t ShardQuorumProcessor::GetMigrateShardID()
{
    return migrateShardID;
}

void ShardQuorumProcessor::TrySplitShard(uint64_t shardID, uint64_t newShardID, ReadBuffer& splitKey)
{
    ShardMessage* itMessage;
    ShardMessage* message;
    
    FOREACH (itMessage, shardMessages)
    {
        if (itMessage->type == SHARDMESSAGE_SPLIT_SHARD && itMessage->shardID == shardID)
        {
            Log_Trace("Not appending shard split");
            return;
        }
    }
    
    Log_Trace("Appending shard split");

    message = new ShardMessage;
    message->SplitShard(shardID, newShardID, splitKey);
    message->clientRequest = NULL;
    message->isBulk = false;
    shardMessages.Append(message);

//    if (quorumContext.GetQuorum()->GetNumNodes() > 1)
//    {
        if (!tryAppend.IsActive())
            EventLoop::Add(&tryAppend);
//    }
//    else
//    {
//        if (!localExecute.IsActive())
//            EventLoop::Add(&localExecute);
//    }
}

void ShardQuorumProcessor::TryTruncateTable(uint64_t tableID, uint64_t newShardID)
{
    ShardMessage* itMessage;
    ShardMessage* message;
    
    FOREACH (itMessage, shardMessages)
    {
        if (itMessage->type == SHARDMESSAGE_TRUNCATE_TABLE && itMessage->tableID == tableID)
        {
            Log_Trace("Not appending truncate table");
            return;
        }
    }
    
    Log_Trace("Appending truncate table");

    message = new ShardMessage;
    message->TruncateTable(tableID, newShardID);
    message->clientRequest = NULL;
    message->isBulk = false;
    shardMessages.Append(message);

//    if (quorumContext.GetQuorum()->GetNumNodes() > 1)
//    {
        if (!tryAppend.IsActive())
            EventLoop::Add(&tryAppend);
//    }
//    else
//    {
//        if (!localExecute.IsActive())
//            EventLoop::Add(&localExecute);
//    }
}

void ShardQuorumProcessor::OnActivation()
{
    if (requestLeaseTimeout.GetDelay() == ACTIVATION_PRIMARYLEASE_REQUEST_TIMEOUT)
        return;
    
    EventLoop::Remove(&requestLeaseTimeout);
    OnRequestLeaseTimeout();

    EventLoop::Remove(&requestLeaseTimeout);
    requestLeaseTimeout.SetDelay(ACTIVATION_PRIMARYLEASE_REQUEST_TIMEOUT);
    EventLoop::Add(&requestLeaseTimeout);
    
    EventLoop::Reset(&activationTimeout);
}

void ShardQuorumProcessor::OnActivationTimeout()
{
    EventLoop::Remove(&requestLeaseTimeout);
    requestLeaseTimeout.SetDelay(NORMAL_PRIMARYLEASE_REQUEST_TIMEOUT);
    EventLoop::Add(&requestLeaseTimeout);
}

void ShardQuorumProcessor::StopReplication()
{
    quorumContext.StopReplication();
}

void ShardQuorumProcessor::ContinueReplication()
{
    quorumContext.ContinueReplication();
}

bool ShardQuorumProcessor::IsCatchupActive()
{
    return catchupWriter.IsActive();
}

uint64_t ShardQuorumProcessor::GetCatchupBytesSent()
{
    return catchupWriter.GetBytesSent();
}

uint64_t ShardQuorumProcessor::GetCatchupBytesTotal()
{
    return catchupWriter.GetBytesTotal();
}

uint64_t ShardQuorumProcessor::GetCatchupThroughput()
{
    return catchupWriter.GetThroughput();
}

void ShardQuorumProcessor::OnShardMigrationClusterMessage(uint64_t nodeID, ClusterMessage& clusterMessage)
{
    ShardMessage*   shardMessage;
    ConfigQuorum*   configQuorum;
    ClusterMessage  pauseMessage;

    configQuorum = shardServer->GetConfigState()->GetQuorum(GetQuorumID());
    ASSERT(configQuorum);

    if (!quorumContext.IsLeader())
    {
        Log_Trace();
        return;
    }

    shardMessage = new ShardMessage();
    shardMessage->clientRequest = NULL;
    shardMessage->isBulk = false;

    switch (clusterMessage.type)
    {
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
            migrateShardID = clusterMessage.dstShardID;
            migrateNodeID = nodeID;
            migrateCache = 0;
            shardMessage->ShardMigrationBegin(clusterMessage.srcShardID, clusterMessage.dstShardID);
            Log_Message("Migrating shard %U into quorum %U (receiving) as %U",
             clusterMessage.srcShardID, GetQuorumID(), clusterMessage.dstShardID);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
            ASSERT(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationSet(clusterMessage.shardID, clusterMessage.key, clusterMessage.value);
            migrateCache += clusterMessage.key.GetLength() + clusterMessage.value.GetLength();
            Log_Debug("migrateCache = %s", HUMAN_BYTES(migrateCache));
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
            ASSERT(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationDelete(clusterMessage.shardID, clusterMessage.key);
            migrateCache += clusterMessage.key.GetLength();
            Log_Debug("ShardMigration DELETE");
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_COMMIT:
            ASSERT(migrateShardID = clusterMessage.shardID);
            Log_Debug("Received shard migration COMMIT");
            shardMessage->ShardMigrationComplete(clusterMessage.shardID);
            break;
        default:
            ASSERT_FAIL();
    }

    shardMessages.Append(shardMessage);
    
    if (migrateCache > DATABASE_REPLICATION_SIZE)
    {
        Log_Debug("Pausing reads from node %U", migrateNodeID);
        pauseMessage.ShardMigrationPause();
        CONTEXT_TRANSPORT->SendClusterMessage(migrateNodeID, pauseMessage);
//        CONTEXT_TRANSPORT->PauseReads(migrateNodeID);        
    }

//    if (configQuorum->activeNodes.GetLength() == 1 && configQuorum->inactiveNodes.GetLength() == 0)
//    {
//        if (!localExecute.IsActive())
//            EventLoop::Add(&localExecute);
//        return;
//    }

    if (!tryAppend.IsActive())
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::OnBlockShard(uint64_t shardID)
{
    // we could be in the middle of an async append loop
    blockedShardID = shardID;
    if (!resumeAppend.IsActive() && !quorumContext.IsAppending())
        BlockShard();
}

uint64_t ShardQuorumProcessor::GetBlockedShardID()
{
    return blockedShardID;
}

void ShardQuorumProcessor::TransformRequest(ClientRequest* request, ShardMessage* message)
{
    message->clientRequest = request;
    message->isBulk = request->isBulk;
    
    switch (request->type)
    {
        case CLIENTREQUEST_SET:
            message->type = SHARDMESSAGE_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_SET_IF_NOT_EXISTS:
            message->type = SHARDMESSAGE_SET_IF_NOT_EXISTS;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_TEST_AND_SET:
            message->type = SHARDMESSAGE_TEST_AND_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->test.Wrap(request->test);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_GET_AND_SET:
            message->type = SHARDMESSAGE_GET_AND_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_ADD:
            message->type = SHARDMESSAGE_ADD;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->number = request->number;
            break;
        case CLIENTREQUEST_APPEND:
            message->type = SHARDMESSAGE_APPEND;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_DELETE:
            message->type = SHARDMESSAGE_DELETE;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            break;
        case CLIENTREQUEST_REMOVE:
            message->type = SHARDMESSAGE_REMOVE;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            break;            
        default:
            ASSERT_FAIL();
    }
}

void ShardQuorumProcessor::ExecuteMessage(uint64_t paxosID, uint64_t commandID,
 ShardMessage* shardMessage, bool ownCommand)
{
    ClusterMessage clusterMessage;
    
    if (shardMessage->type == SHARDMESSAGE_MIGRATION_BEGIN)
    {
        Log_Message("Disabling database merge for the duration of shard migration");
        shardServer->GetDatabaseManager()->GetEnvironment()->SetMergeEnabled(false);
    }

    if (shardMessage->type == SHARDMESSAGE_MIGRATION_SET && migrateCache > 0)
        migrateCache -= (shardMessage->key.GetLength() + shardMessage->value.GetLength());
    else if (shardMessage->type == SHARDMESSAGE_MIGRATION_DELETE && migrateCache > 0)
        migrateCache -= shardMessage->key.GetLength();

    ASSERT(migrateCache >= 0);
    
    if (shardMessage->type == SHARDMESSAGE_MIGRATION_COMPLETE)
    {
        Log_Message("Migration of shard %U complete...", migrateShardID);
        Log_Message("Enabling database merge");
        shardServer->GetDatabaseManager()->GetEnvironment()->SetMergeEnabled(true);

        clusterMessage.ShardMigrationComplete(GetQuorumID(), migrateShardID);
        migrateShardID = 0;
        migrateNodeID = 0;
        migrateCache = 0;

        if (IsPrimary())
            shardServer->BroadcastToControllers(clusterMessage);
    }
    else
        shardServer->GetDatabaseManager()->ExecuteMessage(GetQuorumID(), paxosID, commandID, *shardMessage);
    
    if (!ownCommand)
        return;
        
    if (shardMessage->clientRequest)
    {
        shardMessage->clientRequest->OnComplete(); // request deletes itself
        shardMessage->clientRequest = NULL;
    }
    shardMessages.Delete(shardMessage);
}

void ShardQuorumProcessor::TryAppend()
{
    unsigned        numMessages;
    ShardMessage*   message;
    
    if (shardMessages.GetLength() == 0 || quorumContext.IsAppending())
        return;

    if (resumeAppend.IsActive())
    {
        EventLoop::Add(&tryAppend);
        return;
    }
    
    numMessages = 0;
    Buffer& nextValue = quorumContext.GetNextValue();
    FOREACH (message, shardMessages)
    {
        if (message->isBulk)
            continue;
        
        // make sure split shard messages are replicated by themselves
        if (numMessages != 0 && message->type == SHARDMESSAGE_SPLIT_SHARD)
            break;
        
        message->Append(nextValue);
        nextValue.Appendf(" ");
        numMessages++;

        if (message->type == SHARDMESSAGE_SPLIT_SHARD || nextValue.GetLength() >= DATABASE_REPLICATION_SIZE)
            break;
        
        if (message->clientRequest && SHARD_MIGRATION_WRITER->IsActive() &&
         SHARD_MIGRATION_WRITER->GetShardID() == message->clientRequest->shardID)
            break;
    }

//    Log_Debug("numMessages = %u", numMessages);
//    Log_Debug("length = %s", HUMAN_BYTES(appendValue.GetLength()));
            
    ASSERT(nextValue.GetLength() > 0);
    quorumContext.Append();
}

void ShardQuorumProcessor::OnResumeAppend()
{
    int             read;
    int64_t         prevMigrateCache;
    uint64_t        start;
    ShardMessage*   itShardMessage;
    ShardMessage    shardMessage;
    ClusterMessage  clusterMessage;
    
    prevMigrateCache = migrateCache;
    
    start = NowClock();
    while (appendState.value.GetLength() > 0)
    {
        // parse message
        read = shardMessage.Read(appendState.value);
        ASSERT(read > 0);
        appendState.value.Advance(read);
        ASSERT(appendState.value.GetCharAt(0) == ' ');
        appendState.value.Advance(1);

        if (appendState.currentAppend)
        {
            // find this message in the shardMessages list
            FOREACH (itShardMessage, shardMessages)
                if (itShardMessage->isBulk == false)
                    break;
            ASSERT(itShardMessage != NULL);
        }

        ExecuteMessage(appendState.paxosID, appendState.commandID,
         (appendState.currentAppend ? itShardMessage : &shardMessage), appendState.currentAppend);

        appendState.commandID++;
        
        TRY_YIELD_RETURN(resumeAppend, start);
    }
    
    appendState.Reset();

    if (migrateCache <= DATABASE_REPLICATION_SIZE &&
     prevMigrateCache != migrateCache && migrateNodeID > 0)
    {
        clusterMessage.ShardMigrationResume();
        CONTEXT_TRANSPORT->SendClusterMessage(migrateNodeID, clusterMessage);
//        CONTEXT_TRANSPORT->ResumeReads(migrateNodeID);
    }
    
    if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
        EventLoop::Add(&tryAppend);

    if (blockedShardID != 0)
        BlockShard();
    
    quorumContext.OnAppendComplete();    
}

void ShardQuorumProcessor::LocalExecute()
{
    bool            isSingle;
    uint64_t        start;
    uint64_t        paxosID, commandID;
    ShardMessage*   itMessage;
    ShardMessage*   nextMessage;
    ClusterMessage  clusterMessage;
    
    ASSERT_FAIL();
    
    if (shardServer->GetDatabaseManager()->GetEnvironment()->IsCommiting(GetQuorumID()))
    {
        EventLoop::Add(&localExecute);
        Log_Debug("ExecuteSingles YIELD because commit is active");
        return;
    }
    
    isSingle = (quorumContext.GetQuorum()->GetNumNodes() == 1);
    
    if (isSingle)
        quorumContext.NewPaxosRound();
    
    paxosID = GetPaxosID();
    commandID = 0;
    
    start = NowClock();
    for (itMessage = shardMessages.First(); itMessage != NULL; itMessage = nextMessage)
    {
        nextMessage = shardMessages.Next(itMessage);
        if (!isSingle && !itMessage->isBulk)
            continue;

        ExecuteMessage(paxosID, 0, itMessage, true);
        // removes from shardMessages and clientRequests lists
        
        if (itMessage->clientRequest && SHARD_MIGRATION_WRITER->IsActive() &&
         SHARD_MIGRATION_WRITER->GetShardID() == itMessage->clientRequest->shardID)
            break;

        commandID++;
        TRY_YIELD_BREAK(localExecute, start);
    }

    quorumContext.WriteReplicationState();
    
    shardServer->GetDatabaseManager()->GetEnvironment()->Commit(GetQuorumID());    
}

void ShardQuorumProcessor::BlockShard()
{
    ShardMessage*   itMessage;
    ShardMessage*   nextMessage;
    ConfigShard*    configShard;

    ASSERT(blockedShardID != 0);
    
    Log_Debug("ShardQuorumProcessor::BlockShard()");
    
    for (itMessage = shardMessages.First(); itMessage != NULL; itMessage = nextMessage)
    {
        nextMessage = shardMessages.Next(itMessage);
        if (!itMessage->clientRequest || !itMessage->IsClientWrite())
            continue;
        
        configShard = shardServer->GetConfigState()->GetShard(itMessage->tableID, itMessage->key);
        if (!configShard || configShard->shardID != blockedShardID)
            continue;
        
        itMessage->clientRequest->response.NoService();
        itMessage->clientRequest->OnComplete();
        itMessage->clientRequest = NULL;
        shardMessages.Delete(itMessage);
    }
    
    EventLoop::Reset(&unblockShardTimeout);
}

void ShardQuorumProcessor::OnUnblockShardTimeout()
{
    Log_Debug("ShardQuorumProcessor::OnUnblockShardTimeout()");
    blockedShardID = 0;
}
