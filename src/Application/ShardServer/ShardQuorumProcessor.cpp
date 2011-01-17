#include "ShardQuorumProcessor.h"
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
}

void ShardQuorumProcessor::Init(ConfigQuorum* configQuorum, ShardServer* shardServer_)
{
    shardServer = shardServer_;
//    catchupReader.Init(this);
    isPrimary = false;
    proposalID = 0;
    configID = 0;
    requestedLeaseExpireTime = 0;
    shardMessagesLength = 0;
    quorumContext.Init(configQuorum, this);
    CONTEXT_TRANSPORT->AddQuorumContext(&quorumContext);
    EventLoop::Add(&requestLeaseTimeout);
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

ConfigQuorum* ShardQuorumProcessor::GetConfigQuorum()
{
    return shardServer->GetConfigState()->GetQuorum(GetQuorumID());
}

void ShardQuorumProcessor::OnReceiveLease(ClusterMessage& message)
{
    if (proposalID != message.proposalID)
        return;

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

void ShardQuorumProcessor::OnAppend(uint64_t paxosID, ReadBuffer& value, bool ownAppend)
{
    int             read;
    ShardMessage    message;
    uint64_t        commandID;
    uint64_t        valueLength;
    
    Log_Trace();

    valueLength = value.GetLength();

//    Log_Message("BEGIN --------------");

    commandID = 0;
    while (value.GetLength() > 0)
    {
        read = message.Read(value);
        assert(read > 0);
        value.Advance(read);

        ExecuteMessage(message, paxosID, commandID, ownAppend);
        commandID = 0;
    }
    
    if (ownAppend)
    {
        shardMessagesLength -= valueLength;
        Log_Trace("shardMessagesLength: %U", shardMessagesLength);
        assert(shardMessagesLength >= 0);
    }

//    Log_Message("END --------------");
    
    if (!tryAppend.IsActive() && shardMessages.GetLength() > 0)
        EventLoop::Add(&tryAppend);
}

void ShardQuorumProcessor::OnStartCatchup()
{
//    CatchupMessage  msg;
//
//    if (catchupReader.IsActive() || !quorumContext.IsLeaseKnown())
//        return;
//    
//    quorumContext.StopReplication();
//    
//    msg.CatchupRequest(MY_NODEID, quorumContext.GetQuorumID());
//    
//    CONTEXT_TRANSPORT->SendQuorumMessage(
//     quorumContext.GetLeaseOwner(), quorumContext.GetQuorumID(), msg);
//     
//    catchupReader.Begin();
//    
//    Log_Message("Catchup started from node %U", quorumContext.GetLeaseOwner());
}

void ShardQuorumProcessor::OnCatchupMessage(CatchupMessage& message)
{
//    switch (message.type)
//    {
//        case CATCHUPMESSAGE_REQUEST:
//            if (!catchupWriter.IsActive())
//                catchupWriter.Begin(message);
//            break;
//        case CATCHUPMESSAGE_BEGIN_SHARD:
//            if (catchupReader.IsActive())
//                catchupReader.OnBeginShard(message);
//            break;
//        case CATCHUPMESSAGE_KEYVALUE:
//            if (catchupReader.IsActive())
//                catchupReader.OnKeyValue(message);
//            break;
//        case CATCHUPMESSAGE_COMMIT:
//            if (catchupReader.IsActive())
//            {
//                catchupReader.OnCommit(message);
//                quorumContext.OnCatchupComplete(message.paxosID); // this commits
//                quorumContext.ContinueReplication();
//            }
//            break;
//        case CATCHUPMESSAGE_ABORT:
//            if (catchupReader.IsActive())
//            {
//                catchupReader.OnAbort(message);
//                quorumContext.ContinueReplication();
//            }
//            break;
//        default:
//            ASSERT_FAIL();
//    }
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
}

void ShardQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ShardMessage*   message;
    Buffer          singleBuffer;

    if (!quorumContext.IsLeader())
    {
        Log_Trace();
        request->response.Failed();
        request->OnComplete();
        return;
    }

    if (request->type == CLIENTREQUEST_GET)
    {
        shardServer->GetDatabaseManager()->OnClientReadRequest(request);        
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

void ShardQuorumProcessor::TryReplicationCatchup()
{
    // this is called if we're an inactive node and we should probably try to catchup
//    
//    if (catchupReader.IsActive())
//        return;
//    
//    quorumContext.TryReplicationCatchup();
}

void ShardQuorumProcessor::TrySplitShard(uint64_t shardID, uint64_t newShardID, ReadBuffer& splitKey)
{
    ShardMessage*   it;
    Buffer          singleBuffer;
    
    FOREACH(it, shardMessages)
    {
        if (it->type == SHARDMESSAGE_SPLIT_SHARD && it->shardID == shardID)
            return;
    }
    
    it = new ShardMessage;
    it->SplitShard(shardID, newShardID, splitKey);
    shardMessages.Append(it);
    
    it->Write(singleBuffer);
    shardMessagesLength += singleBuffer.GetLength();
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

    if (ownAppend)
        fromClient = shardMessages.First()->fromClient;
    else
        fromClient = false;

    request = NULL;
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
    Buffer          singleBuffer;
    ShardMessage*   it;
    bool            first;
    unsigned        numMessages;
    
    assert(shardMessages.GetLength() > 0);
    
    numMessages = 0;
    
    if (!quorumContext.IsAppending() && shardMessages.GetLength() > 0)
    {
        Buffer& value = quorumContext.GetNextValue();        
        
        first = true;
        FOREACH(it, shardMessages)
        {
            // make sure split shard messages are replicated by themselves
            if (!first && it->type == SHARDMESSAGE_SPLIT_SHARD)
                break;
            
            if (first)
                first = false;

            singleBuffer.Clear();
            it->Write(singleBuffer);
            if (value.GetLength() + 1 + singleBuffer.GetLength() < DATABASE_REPLICATION_SIZE)
            {
                value.Appendf("%B", &singleBuffer);
                numMessages++;
                // make sure split shard messages are replicated by themselves
                if (it->type == SHARDMESSAGE_SPLIT_SHARD)
                    break;
            }
            else
                break;
        }
        
        Log_Message("numMessages = %u", numMessages);
        
        assert(value.GetLength() > 0);
        
        quorumContext.Append();
    }
}
