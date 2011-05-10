#include "ShardCatchupWriter.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"
#include "Framework/Replication/ReplicationConfig.h"

ShardCatchupWriter::ShardCatchupWriter()
{
    onTimeout.SetCallable(MFUNC(ShardCatchupWriter, OnTimeout));
    onTimeout.SetDelay(SHARD_CATCHUP_WRITER_DELAY);
    Reset();
}

ShardCatchupWriter::~ShardCatchupWriter()
{
    if (cursor != NULL)
        delete cursor;
}

void ShardCatchupWriter::Init(ShardQuorumProcessor* quorumProcessor_)
{
    quorumProcessor = quorumProcessor_;
    
    environment = quorumProcessor->GetShardServer()->GetDatabaseManager()->GetEnvironment();

    Reset();
}

void ShardCatchupWriter::Reset()
{
    cursor = NULL;
    isActive = false;
    bytesSent = 0;
    bytesTotal = 0;
    startTime = 0;
    prevBytesSent = 0;
    EventLoop::Remove(&onTimeout);
}

bool ShardCatchupWriter::IsActive()
{
    return isActive;
}

uint64_t ShardCatchupWriter::GetBytesSent()
{
    return bytesSent;
}

uint64_t ShardCatchupWriter::GetBytesTotal()
{
    return bytesTotal;
}

uint64_t ShardCatchupWriter::GetThroughput()
{
    uint64_t now;
    
    now = NowClock();
    
    if (now > startTime)
        return (uint64_t)(bytesSent / ((now - startTime)/1000.0));
    else
        return 0;
}

void ShardCatchupWriter::Begin(CatchupMessage& request)
{
    uint64_t* it;

    ASSERT(!isActive);   
    ASSERT(quorumProcessor != NULL);
    ASSERT(cursor == NULL);

    isActive = true;
    nodeID = request.nodeID;
    quorumID = request.quorumID;
    paxosID = quorumProcessor->GetPaxosID() - 1;
    
    EventLoop::Add(&onTimeout);

    ShardQuorumProcessor::ShardList& shards = quorumProcessor->GetConfigQuorum()->shards;
    FOREACH (it, shards)
        bytesTotal += environment->GetSize(QUORUM_DATABASE_DATA_CONTEXT, *it);
    startTime = NowClock();

    if (quorumProcessor->GetConfigQuorum()->shards.GetLength() == 0)
        SendCommit();
    else
        SendFirst();
    
    if (isActive)
        CONTEXT_TRANSPORT->RegisterWriteReadyness(nodeID, MFUNC(ShardCatchupWriter, OnWriteReadyness));
}

void ShardCatchupWriter::Abort()
{
    CatchupMessage msg;
    
    Log_Message("Catchup aborted");
    
    msg.Abort();
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);

    if (cursor != NULL)
    {
        delete cursor;
        cursor = NULL;
    }
    
    CONTEXT_TRANSPORT->UnregisterWriteReadyness(nodeID, MFUNC(ShardCatchupWriter, OnWriteReadyness));

    Reset();
}

void ShardCatchupWriter::SendFirst()
{
    CatchupMessage      msg;
    ReadBuffer          key;
    ReadBuffer          value;

    ASSERT(quorumProcessor->GetConfigQuorum()->shards.GetLength() > 0);
    shardID = *(quorumProcessor->GetConfigQuorum()->shards.First());

    ASSERT(cursor == NULL);
    cursor = environment->GetBulkCursor(QUORUM_DATABASE_DATA_CONTEXT, shardID);
    ASSERT(cursor != NULL);

    msg.BeginShard(shardID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);


    // send first KV
    kv = cursor->First();
    if (kv == NULL)
    {
        SendNext();
        return;
    }

    TransformKeyValue(kv, msg);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
    Log_Debug("Sending BEGIN SHARD %U", shardID);
}

void ShardCatchupWriter::SendNext()
{
    uint64_t*           itShardID;
    CatchupMessage      msg;

    ASSERT(cursor != NULL);
    if (kv != NULL)
    {
        kv = cursor->Next(kv);
        if (kv)
        {
            TransformKeyValue(kv, msg);
            CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
            return;
        }
    }

    // kv is NULL, at end of current shard
    delete cursor;
    cursor = NULL;
    
    itShardID = NextShard();
    
    if (!itShardID)
    {
        SendCommit(); // there is no next shard, send commit
        return;
    }    

    shardID = *itShardID;

    delete cursor;
    cursor = quorumProcessor->GetShardServer()->GetDatabaseManager()->GetEnvironment()->GetBulkCursor(
     QUORUM_DATABASE_DATA_CONTEXT, shardID);

    msg.BeginShard(shardID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
    Log_Debug("Sending BEGIN SHARD %U", shardID);

    // send first KV
    kv = cursor->First();
    if (!kv)
    {
        SendNext();
        return;
    }
    
    TransformKeyValue(kv, msg);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
}

void ShardCatchupWriter::SendCommit()
{
    CatchupMessage msg;
    
    msg.Commit(paxosID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
    Log_Debug("Sending COMMIT with paxosID = %U", paxosID);

    if (cursor != NULL)
    {
        delete cursor;
        cursor = NULL;
    }

    CONTEXT_TRANSPORT->UnregisterWriteReadyness(nodeID, MFUNC(ShardCatchupWriter, OnWriteReadyness));

    Reset();
}

void ShardCatchupWriter::OnWriteReadyness()
{
    uint64_t bytesBegin;

    bytesBegin = bytesSent;

    SendNext();

    while (bytesSent < bytesBegin + SHARD_CATCHUP_WRITER_GRAN)
    {
        if (!cursor)
            break;
        SendNext();
    }
}

uint64_t* ShardCatchupWriter::NextShard()
{
    uint64_t* it;
    
    ShardQuorumProcessor::ShardList& shards = quorumProcessor->GetConfigQuorum()->shards;

    FOREACH (it, shards)
    {
        if (*it == shardID)
            break;
    }
    
    if (!it)
        return NULL; // shard was deleted

    it = shards.Next(it);

    return it;
}

void ShardCatchupWriter::TransformKeyValue(StorageKeyValue* kv, CatchupMessage& msg)
{
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
    {
        msg.Set(kv->GetKey(), kv->GetValue());
        bytesSent += kv->GetKey().GetLength() + kv->GetValue().GetLength();
    }
    else
    {
        msg.Delete(kv->GetKey());
        bytesSent += kv->GetKey().GetLength();
    }
}

void ShardCatchupWriter::OnTimeout()
{
    ConfigState*    configState;
    ConfigQuorum*   configQuorum;
    
    // check the destination nodeID is still in the quorum
    configState = quorumProcessor->GetShardServer()->GetConfigState();
    configQuorum = configState->GetQuorum(quorumID);
    if (configQuorum == NULL)
    {
        Abort();
        return;
    }
    if (!configQuorum->IsMember(MY_NODEID))
    {
        Abort();
        return;
    }
    if (!configQuorum->IsMember(nodeID))
    {
        Abort();
        return;
    }

    if (bytesSent == prevBytesSent)
    {
        Abort();
        return;
    }

    prevBytesSent = bytesSent;
    EventLoop::Add(&onTimeout);
}
