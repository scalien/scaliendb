#include "ShardQuorumProcessor.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"

static bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

ShardQuorumProcessor::ShardQuorumProcessor()
{
    prev = next = this;

    requestLeaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnRequestLeaseTimeout));
    requestLeaseTimeout.SetDelay(PRIMARYLEASE_REQUEST_TIMEOUT);
    
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
    proposalID = 0;
    configID = 0;
    requestedLeaseExpireTime = 0;
    ownAppend = false;
    paxosID = 0;
    commandID = 0;
    migrateShardID = 0;
    quorumContext.Init(configQuorum, this);
    CONTEXT_TRANSPORT->AddQuorumContext(&quorumContext);
    EventLoop::Add(&requestLeaseTimeout);
}

void ShardQuorumProcessor::Shutdown()
{
    ClientRequest*  request;
    ShardMessage*   message;
    
    FOREACH_FIRST(message, shardMessages)
    {
        shardMessages.Remove(message);
        delete message;
    }
    
    FOREACH_FIRST(request, clientRequests)
    {
        clientRequests.Remove(request);
        request->response.NoService();
        request->OnComplete();
    }
    
    if (requestLeaseTimeout.IsActive())
        EventLoop::Remove(&requestLeaseTimeout);

    if (leaseTimeout.IsActive())
        EventLoop::Remove(&leaseTimeout);

    if (tryAppend.IsActive())
        EventLoop::Remove(&tryAppend);

    if (resumeAppend.IsActive())
        EventLoop::Remove(&resumeAppend);
    
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
    SortedList<uint64_t>    activeNodes;
    uint64_t*               it;
    
    if (proposalID != message.proposalID)
        return;

    Log_Trace("received lease for quorum %U", GetQuorumID());

    SortedList<uint64_t>& shards = shardServer->GetConfigState()->GetQuorum(GetQuorumID())->shards;
    if (shards != message.shards)
        return;
    
    isPrimary = true;
    configID = message.configID;
    
    FOREACH(it, message.activeNodes)
        activeNodes.Add(*it);
    quorumContext.SetActiveNodes(activeNodes);
        
    shards = message.shards;
    
    leaseTimeout.SetExpireTime(requestedLeaseExpireTime);
    EventLoop::Reset(&leaseTimeout);
    
    quorumContext.OnLearnLease();
    
    if (message.watchingPaxosID && shardMessages.GetLength() == 0)
        quorumContext.AppendDummy();
}

void ShardQuorumProcessor::OnAppend(uint64_t paxosID_, ReadBuffer& value_, bool ownAppend_)
{
    paxosID = paxosID_;
    commandID = 0;
    
    valueBuffer.Write(value_);
    value.Wrap(valueBuffer);
    ownAppend = ownAppend_;

    OnResumeAppend();
}

void ShardQuorumProcessor::OnStartCatchup()
{
    CatchupMessage  msg;

    if (catchupReader.IsActive() || !quorumContext.IsLeaseKnown())
        return;
    
    quorumContext.StopReplication();
    
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
    ClusterMessage msg;
    
    Log_Trace();
    
    proposalID = REPLICATION_CONFIG->NextProposalID(proposalID);
    msg.RequestLease(MY_NODEID, quorumContext.GetQuorumID(), proposalID,
     GetPaxosID(), configID, PAXOSLEASE_MAX_LEASE_TIME);
    
    shardServer->BroadcastToControllers(msg);

    requestedLeaseExpireTime = EventLoop::Now() + PAXOSLEASE_MAX_LEASE_TIME;
    if (!requestLeaseTimeout.IsActive())
        EventLoop::Add(&requestLeaseTimeout);
}

void ShardQuorumProcessor::OnLeaseTimeout()
{
    ClientRequest*  itRequest, *nextRequest;
    ShardMessage*   itMessage, *nextMessage;
        
    for (itRequest = clientRequests.First(); itRequest != NULL; itRequest = nextRequest)
    {
        nextRequest = clientRequests.Next(itRequest);

        if (!itRequest->isBulk)
        {
            clientRequests.Remove(itRequest);
            itRequest->response.NoService();
            itRequest->OnComplete();
        }
    }

    for (itMessage = shardMessages.First(); itMessage != NULL; itMessage = nextMessage)
    {
        nextMessage = shardMessages.Next(itMessage);
        
        if (!itMessage->isBulk)
        {
            shardMessages.Remove(itMessage);
            delete itMessage;
        }
    }
    
    isPrimary = false;
    migrateShardID = 0;
    
    if (catchupWriter.IsActive())
        catchupWriter.Abort();
}

void ShardQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ShardMessage*   message;

    if (!quorumContext.IsLeader() && !request->isBulk)
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
    
    clientRequests.Append(request);
    shardMessages.Append(message);

    if (request->isBulk || quorumContext.GetQuorum()->GetNumNodes() == 1)
    {
        if (!localExecute.IsActive())
            EventLoop::Add(&localExecute);
        return;
    }
    
    if (!tryAppend.IsActive())
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::OnClientClose(ClientSession* /*session*/)
{
}

void ShardQuorumProcessor::SetActiveNodes(SortedList<uint64_t>& activeNodes)
{
    quorumContext.SetActiveNodes(activeNodes);

//    if (quorumContext.GetQuorum()->GetNumNodes() > 1)
//    {
//        if (localExecute.IsActive())
//        {
//            EventLoop::Remove(&localExecute);
//            if (!tryAppend.IsActive())
//                EventLoop::Add(&tryAppend);
//        }
//    }
}

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
    ShardMessage*   it;
    
    FOREACH(it, shardMessages)
    {
        if (it->type == SHARDMESSAGE_SPLIT_SHARD && it->shardID == shardID)
        {
            Log_Trace("Not appending shard split");
            return;
        }
    }
    
    Log_Trace("Appending shard split");

    it = new ShardMessage;
    it->SplitShard(shardID, newShardID, splitKey);
    it->fromClient = false;
    it->isBulk = false;
    shardMessages.Append(it);

    if (quorumContext.GetQuorum()->GetNumNodes() > 1)
    {
        if (!tryAppend.IsActive())
            EventLoop::Add(&tryAppend);
    }
    else
    {
        if (!localExecute.IsActive())
            EventLoop::Add(&localExecute);
    }
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

void ShardQuorumProcessor::OnShardMigrationClusterMessage(ClusterMessage& clusterMessage)
{
    ShardMessage*   shardMessage;
    ClusterMessage  completeMessage;

    if (!quorumContext.IsLeader())
    {
        Log_Trace();
        return;
    }

    if (clusterMessage.type == CLUSTERMESSAGE_SHARDMIGRATION_COMMIT)
    {
        assert(migrateShardID = clusterMessage.shardID);
        completeMessage.ShardMigrationComplete(GetQuorumID(), migrateShardID);
        shardServer->BroadcastToControllers(completeMessage);
        migrateShardID = 0;
        Log_Message("Migration of shard %U complete...", clusterMessage.shardID);
        return;
    }
    
    shardMessage = new ShardMessage();
    shardMessage->fromClient = false;

    switch (clusterMessage.type)
    {
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
            migrateShardID = clusterMessage.shardID;
            shardMessage->ShardMigrationBegin(clusterMessage.shardID);
            Log_Message("Migrating shard %U into quorum %U (receiving)", clusterMessage.shardID, GetQuorumID());
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
            assert(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationSet(clusterMessage.shardID, clusterMessage.key, clusterMessage.value);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
            assert(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationDelete(clusterMessage.shardID, clusterMessage.key);
            Log_Debug("ShardMigration DELETE");
            break;
        default:
            ASSERT_FAIL();
    }

    shardMessages.Append(shardMessage);

    if (!tryAppend.IsActive())
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::TransformRequest(ClientRequest* request, ShardMessage* message)
{
    message->fromClient = true;
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

void ShardQuorumProcessor::ExecuteMessage(ShardMessage& message,
 uint64_t paxosID, uint64_t commandID, bool ownAppend)
{
    ClientRequest*  request;
    ShardMessage*   itMessage;
    bool            fromClient;

    request = NULL;

    // TODO: this is not a complete solution
    if (shardMessages.GetLength() == 0)
    {
        shardServer->GetDatabaseManager()->ExecuteMessage(paxosID, commandID, message, request);
        return;
    }

    if (ownAppend)
    {
        FOREACH(itMessage, shardMessages)
        {
            if (itMessage->isBulk == false)
                break;
        }
        assert(itMessage != NULL);
        fromClient = itMessage->fromClient;
    }
    else
        fromClient = false;

    if (ownAppend && fromClient)
    {
        assert(shardMessages.GetLength() > 0);
        assert(itMessage->type == message.type);
        assert(clientRequests.GetLength() > 0);
        request = clientRequests.First();
        request->response.OK();
    }
    
    shardServer->GetDatabaseManager()->ExecuteMessage(paxosID, commandID, message, request);

    if (ownAppend)
    {
        if (fromClient)
        {
            clientRequests.Remove(request);
            request->OnComplete(); // request deletes itself
        }
        shardMessages.Delete(itMessage);
    }
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
    FOREACH(message, shardMessages)
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
    }

//    Log_Debug("numMessages = %u", numMessages);
//    Log_Debug("length = %s", HUMAN_BYTES(appendValue.GetLength()));
            
    assert(nextValue.GetLength() > 0);
    quorumContext.Append();
}

void ShardQuorumProcessor::OnResumeAppend()
{
    int             read;
    uint64_t        valueLength;
    uint64_t        start;
    ShardMessage    message;
    
    Log_Trace();

//    Log_Debug("OnResumeAppend BEGIN");

    valueLength = value.GetLength();

    start = NowClock();
    while (value.GetLength() > 0)
    {
        read = message.Read(value);
        assert(read > 0);
        value.Advance(read);
        assert(value.GetCharAt(0) == ' ');
        value.Advance(1);

        ExecuteMessage(message, paxosID, commandID, ownAppend);
        commandID++;

        if (NowClock() - start >= YIELD_TIME)
        {
            // let other code run every YIELD_TIME msec
            if (resumeAppend.IsActive())
                STOP_FAIL(1, "Program bug: resumeAppend.IsActive() should be false.");
            EventLoop::Add(&resumeAppend);
//            Log_Debug("OnResumeAppend YIELD");
            return;
        }
    }
        
    ownAppend = false;
    paxosID = 0;
    commandID = 0;
    valueBuffer.Reset();
    value.Reset();

    if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
        EventLoop::Add(&tryAppend);
    
    quorumContext.OnAppendComplete();
//    Log_Debug("OnResumeAppend END");
}

void ShardQuorumProcessor::LocalExecute()
{
    bool            isSingle;
    uint64_t        start;
    uint64_t        paxosID, commandID;
    ShardMessage*   itMessage, *nextMessage;
    ClientRequest*  itRequest, *nextRequest;
    
    isSingle = (quorumContext.GetQuorum()->GetNumNodes() == 1);
    
    if (isSingle)
        quorumContext.NewPaxosRound();
    
    paxosID = GetPaxosID();
    commandID = 0;
    
    start = NowClock();
    for (itMessage = shardMessages.First(), itRequest = clientRequests.First();
     itMessage != NULL;
     itMessage = nextMessage, itRequest = nextRequest)
    {
        nextMessage = shardMessages.Next(itMessage);
        if (itMessage->fromClient)
            nextRequest = clientRequests.Next(itRequest);
        else
            nextRequest = itRequest;
        
        if (!isSingle && !itMessage->isBulk)
            continue;
        shardMessages.Remove(itMessage);
        if (itMessage->fromClient)
            clientRequests.Remove(itRequest);

        shardServer->GetDatabaseManager()->ExecuteMessage(paxosID, 0, *itMessage,
         (itMessage->fromClient ? itRequest : NULL));

        if (itMessage->fromClient)
            itRequest->OnComplete(); // request deletes itself
        delete itMessage;
        
        commandID++;

        if (NowClock() - start >= YIELD_TIME)
        {
            // let other code run every YIELD_TIME msec
            if (localExecute.IsActive())
                STOP_FAIL(1, "Program bug: localExecute.IsActive() should be false.");
            EventLoop::Add(&localExecute);
            Log_Debug("ExecuteSingles YIELD");
            break;
        }
    }

    quorumContext.WriteReplicationState();
    
    shardServer->GetDatabaseManager()->GetEnvironment()->Commit();    
}
