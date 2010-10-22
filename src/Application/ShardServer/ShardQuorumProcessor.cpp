#include "ShardQuorumProcessor.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"

ShardQuorumProcessor::ShardQuorumProcessor()
{
    next = prev = this;

    requestLeaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnRequestLeaseTimeout));
    requestLeaseTimeout.SetDelay(PRIMARYLEASE_REQUEST_TIMEOUT);
    
    leaseTimeout.SetCallable(MFUNC(ShardQuorumProcessor, OnLeaseTimeout));
}

void ShardQuorumProcessor::Init(ConfigQuorum* configQuorum, ShardServer* shardServer_)
{
    shardServer = shardServer_;
    catchupReader.Init(this);
    isPrimary = false;
    proposalID = 0;
    quorumContext.Init(configQuorum, this,
     shardServer->GetDatabaseAdapter()->GetQuorumTable(configQuorum->quorumID));
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

ShardQuorumProcessor::ShardList& ShardQuorumProcessor::GetShards()
{
    return shards;
}

void ShardQuorumProcessor::OnReceiveLease(ClusterMessage& message)
{
    if (proposalID != message.proposalID)
        return;
    
    isPrimary = true;
    configID = message.configID;
    activeNodes = message.activeNodes;
    quorumContext.SetActiveNodes(activeNodes);
    
    leaseTimeout.SetExpireTime(requestedLeaseExpireTime);
    EventLoop::Reset(&leaseTimeout);
    
    quorumContext.OnLearnLease();
}

void ShardQuorumProcessor::OnAppend(ReadBuffer& value, bool ownAppend)
{
    int             read;
    ShardMessage    message;
    
    Log_Trace();
    
    while (value.GetLength() > 0)
    {
        read = message.Read(value);
        assert(read > 0);
        value.Advance(read);

        ProcessMessage(message, ownAppend);
    }
    
    if (shardMessages.GetLength() > 0)
        TryAppend();
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
    
    Log_Message("Catchup started from node %" PRIu64 "", quorumContext.GetLeaseOwner());
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
        case CATCHUPMESSAGE_KEYVALUE:
            if (catchupReader.IsActive())
                catchupReader.OnKeyValue(message);
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
    isPrimary = false;
}

void ShardQuorumProcessor::OnClientRequest(ClientRequest* request)
{
    ShardMessage* message;

    if (!quorumContext.IsLeader())
    {
        request->response.Failed();
        request->OnComplete();
        return;
    }

    if (request->type == CLIENTREQUEST_GET)
        return OnClientRequestGet(request);

    message = new ShardMessage;
    TransformRequest(request, message);
    
    Log_Trace("message.type = %c, message.key = %.*s", message->type, P(&message->key));
    
    clientRequests.Append(request);
    shardMessages.Append(message);
    TryAppend();
}

void ShardQuorumProcessor::OnClientClose(ClientSession* /*session*/)
{
}

void ShardQuorumProcessor::SetActiveNodes(ConfigQuorum::NodeList& activeNodes)
{
    quorumContext.SetActiveNodes(activeNodes);
}

void ShardQuorumProcessor::TryReplicationCatchup()
{
    // this is called if we're an inactive node and we should probably try to catchup
    
    if (catchupReader.IsActive())
        return;
    
    quorumContext.TryReplicationCatchup();
}

void ShardQuorumProcessor::OnClientRequestGet(ClientRequest* request)
{
    StorageTable*   table;
    ReadBuffer      key;
    ReadBuffer      value;

    table = shardServer->GetDatabaseAdapter()->GetTable(request->tableID);
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

void ShardQuorumProcessor::TransformRequest(ClientRequest* request, ShardMessage* message)
{
    switch (request->type)
    {
        case CLIENTREQUEST_SET:
            message->type = SHARDMESSAGE_SET;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            message->value.Wrap(request->value);
            break;
        case CLIENTREQUEST_DELETE:
            message->type = SHARDMESSAGE_DELETE;
            message->tableID = request->tableID;
            message->key.Wrap(request->key);
            break;
        default:
            ASSERT_FAIL();
    }
}

void ShardQuorumProcessor::ProcessMessage(ShardMessage& message, bool ownAppend)
{
    StorageTable*       table;
    ClientRequest*      request;

    table = shardServer->GetDatabaseAdapter()->GetTable(message.tableID);
    if (!table)
        ASSERT_FAIL();
    
    request = NULL;
    if (ownAppend)
    {
        assert(shardMessages.GetLength() > 0);
        assert(shardMessages.First()->type == message.type);
        assert(clientRequests.GetLength() > 0);
        request = clientRequests.First();
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
        clientRequests.Remove(request);
        request->OnComplete(); // request deletes itself
        shardMessages.Delete(shardMessages.First());
    }
}

void ShardQuorumProcessor::TryAppend()
{
    Buffer          singleBuffer;
    ShardMessage*   it;
    
    assert(shardMessages.GetLength() > 0);
    
    if (!quorumContext.IsAppending() && shardMessages.GetLength() > 0)
    {
        Buffer& value = quorumContext.GetNextValue();
        FOREACH(it, shardMessages)
        {
            singleBuffer.Clear();
            it->Write(singleBuffer);
            if (value.GetLength() + 1 + singleBuffer.GetLength() < DATABASE_REPLICATION_SIZE)
                value.Appendf("%B", &singleBuffer);
            else
                break;
        }
        
        assert(value.GetLength() > 0);
        
        quorumContext.Append();
    }
}
