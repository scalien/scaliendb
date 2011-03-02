#include "ShardQuorumProcessor.h"
#include "System/Compress/Compressor.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"

ShardQuorumProcessor::ShardQuorumProcessor()
{
    prev = next = this;

    requestLeaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnRequestLeaseTimeout));
    requestLeaseTimeout.SetDelay(PRIMARYLEASE_REQUEST_TIMEOUT);
    
    leaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnLeaseTimeout));
    tryAppend.SetCallable(MFUNC(ShardQuorumProcessor, TryAppend));
    resumeAppend.SetCallable(MFUNC(ShardQuorumProcessor, OnResumeAppend));
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
    shardMessagesLength = 0;
    ownAppend = false;
    paxosID = 0;
    commandID = 0;
//    isShardMigrationActive = false;
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
    if (proposalID != message.proposalID)
        return;

    Log_Trace("received lease for quorum %U", GetQuorumID());

    SortedList<uint64_t>& shards = shardServer->GetConfigState()->GetQuorum(GetQuorumID())->shards;
    if (shards != message.shards)
        return;
    
    isPrimary = true;
    configID = message.configID;
    activeNodes = message.activeNodes;
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
    int         read;
    uint64_t    uncompressedLength;
    Compressor  compressor;
    
    paxosID = paxosID_;
    commandID = 0;
    
    read = value_.Readf("%U:", &uncompressedLength);
    assert(read > 1);
    value_.Advance(read);
    assert(compressor.Uncompress(value_, valueBuffer, uncompressedLength));
    
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

void ShardQuorumProcessor::OnResumeAppend()
{
    int             read;
    uint64_t        valueLength;
    uint64_t        start;
    ShardMessage    message;
    
    Log_Trace();

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
            return;
        }
    }
    
    // TODO:
    if (ownAppend && shardMessages.GetLength() > 0)
    {
        shardMessagesLength -= valueLength;
        Log_Trace("shardMessagesLength: %U", shardMessagesLength);
        //assert(shardMessagesLength >= 0);
        // TODO: HACK
        shardMessagesLength = 0;
    }
    
    ownAppend = false;
    paxosID = 0;
    commandID = 0;
    valueBuffer.Reset();
    value.Reset();

    if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
        EventLoop::Add(&tryAppend);
    
    quorumContext.OnAppendComplete();
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
    ClientRequest*  itRequest;
    ShardMessage*   itMessage;
        
    for (itRequest = clientRequests.First(); itRequest != NULL; itRequest = clientRequests.First())
    {
        clientRequests.Remove(itRequest);
        itRequest->response.NoService();
        itRequest->OnComplete();
    }

    for (itMessage = shardMessages.First(); itMessage != NULL; itMessage = shardMessages.First())
    {
        shardMessages.Remove(itMessage);
        delete itMessage;
    }
    
    shardMessagesLength = 0;
    isPrimary = false;
//    isShardMigrationActive = false;
    migrateShardID = 0;
    
    if (catchupWriter.IsActive())
        catchupWriter.Abort();
}

void ShardQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ShardMessage*   message;
    Buffer          singleBuffer;

    if (!quorumContext.IsLeader())
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
    
    if (request->type == CLIENTREQUEST_SUBMIT)
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
    
    message->Write(singleBuffer);
    shardMessagesLength += singleBuffer.GetLength();
    
    assert(shardMessagesLength >= 0);
    
    if (!tryAppend.IsActive())
    {
        Log_Trace("shardMessagesLength: %U", shardMessagesLength);
        EventLoop::Add(&tryAppend);
    }    
}

void ShardQuorumProcessor::OnClientClose(ClientSession* /*session*/)
{
}

void ShardQuorumProcessor::SetActiveNodes(List<uint64_t>& activeNodes)
{
    quorumContext.SetActiveNodes(activeNodes);
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

//bool ShardQuorumProcessor::IsShardMigrationActive()
//{
//    return isShardMigrationActive;
//}

uint64_t ShardQuorumProcessor::GetMigrateShardID()
{
    return migrateShardID;
}

void ShardQuorumProcessor::TrySplitShard(uint64_t shardID, uint64_t newShardID, ReadBuffer& splitKey)
{
    ShardMessage*   it;
    Buffer          singleBuffer;
    
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
    shardMessages.Append(it);
    
    it->Write(singleBuffer);
    shardMessagesLength += singleBuffer.GetLength();

    if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
        EventLoop::Add(&tryAppend);
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
    Buffer          singleBuffer;

    if (!quorumContext.IsLeader())
    {
        Log_Trace();
        return;
    }

    if (clusterMessage.type == CLUSTERMESSAGE_SHARDMIGRATION_COMMIT)
    {
//        assert(isShardMigrationActive);
        assert(migrateShardID = clusterMessage.shardID);
        completeMessage.ShardMigrationComplete(GetQuorumID(), migrateShardID);
        shardServer->BroadcastToControllers(completeMessage);
//        isShardMigrationActive = false;
        migrateShardID = 0;
        Log_Message("Migration of shard %U complete...", clusterMessage.shardID);
        return;
    }
    
    shardMessage = new ShardMessage();
    shardMessage->fromClient = false;

    switch (clusterMessage.type)
    {
        case CLUSTERMESSAGE_SHARDMIGRATION_BEGIN:
//            assert(isShardMigrationActive == false);
//            isShardMigrationActive = true;
            migrateShardID = clusterMessage.shardID;
            shardMessage->ShardMigrationBegin(clusterMessage.shardID);
            Log_Message("Migrating shard %U into quorum %U (receiving)", clusterMessage.shardID, GetQuorumID());
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_SET:
//            assert(isShardMigrationActive);
            assert(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationSet(clusterMessage.shardID, clusterMessage.key, clusterMessage.value);
            break;
        case CLUSTERMESSAGE_SHARDMIGRATION_DELETE:
//            assert(isShardMigrationActive);
            assert(migrateShardID = clusterMessage.shardID);
            shardMessage->ShardMigrationDelete(clusterMessage.shardID, clusterMessage.key);
            Log_Debug("ShardMigration DELETE");
            break;
        default:
            ASSERT_FAIL();
    }

    shardMessages.Append(shardMessage);

    shardMessage->Write(singleBuffer);
    shardMessagesLength += singleBuffer.GetLength();

    if (!tryAppend.IsActive())
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::TransformRequest(ClientRequest* request, ShardMessage* message)
{
    message->fromClient = true;
    
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
    bool            fromClient;

    request = NULL;

    // TODO: this is not a complete solution
    if (shardMessages.GetLength() == 0)
    {
        shardServer->GetDatabaseManager()->ExecuteMessage(paxosID, commandID, message, request);
        return;
    }

    if (ownAppend)
        fromClient = shardMessages.First()->fromClient;
    else
        fromClient = false;

    if (ownAppend && fromClient)
    {
        assert(shardMessages.GetLength() > 0);
        assert(shardMessages.First()->type == message.type);
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
        shardMessages.Delete(shardMessages.First());
    }
}

void ShardQuorumProcessor::TryAppend()
{
    bool            first;
    unsigned        numMessages;
    Buffer          singleBuffer;
    Buffer          uncompressed;
    Buffer          compressed;
    ShardMessage*   it;
    Compressor      compressor;
        
    if (shardMessages.GetLength() == 0)
        return;

    if (resumeAppend.IsActive())
    {
        EventLoop::Add(&tryAppend);
        return;
    }
    
    numMessages = 0;
    
    if (!quorumContext.IsAppending() && shardMessages.GetLength() > 0)
    {
        first = true;
        FOREACH(it, shardMessages)
        {
            // make sure split shard messages are replicated by themselves
            if (!first && it->type == SHARDMESSAGE_SPLIT_SHARD)
                break;
            
            singleBuffer.Clear();
            it->Write(singleBuffer);
            if (first || uncompressed.GetLength() + 1 + singleBuffer.GetLength() < DATABASE_UNCOMPRESSED_REPLICATION_SIZE)
            {
                uncompressed.Appendf("%B ", &singleBuffer);
                numMessages++;
                // make sure split shard messages are replicated by themselves
                if (it->type == SHARDMESSAGE_SPLIT_SHARD)
                    break;
            }
            else
                break;

            if (first)
                first = false;
        }
        
//        Log_Debug("numMessages = %u", numMessages);
                
        assert(uncompressed.GetLength() > 0);

        assert(compressor.Compress(uncompressed, compressed));

        quorumContext.GetNextValue().Writef("%u:%B", uncompressed.GetLength(), &compressed);
        
        quorumContext.Append();
    }
}
