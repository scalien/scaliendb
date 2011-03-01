#include "ShardCatchupWriter.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

ShardCatchupWriter::ShardCatchupWriter()
{
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
    return bytesSent / ((NowClock() - startTime)/1000);
}

void ShardCatchupWriter::Begin(CatchupMessage& request)
{
    uint64_t* it;

    assert(!isActive);   
    assert(quorumProcessor != NULL);
    assert(cursor == NULL);

    isActive = true;
    nodeID = request.nodeID;
    quorumID = request.quorumID;
    paxosID = quorumProcessor->GetPaxosID() - 1;

    ShardQuorumProcessor::ShardList& shards = quorumProcessor->GetConfigQuorum()->shards;
    FOREACH(it, shards)
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

    assert(quorumProcessor->GetConfigQuorum()->shards.GetLength() > 0);
    shardID = *(quorumProcessor->GetConfigQuorum()->shards.First());

    assert(cursor == NULL);
    cursor = environment->GetBulkCursor(QUORUM_DATABASE_DATA_CONTEXT, shardID);
    assert(cursor != NULL);

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

    assert(cursor != NULL);
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
    SendNext();
}

uint64_t* ShardCatchupWriter::NextShard()
{
    uint64_t* it;
    
    ShardQuorumProcessor::ShardList& shards = quorumProcessor->GetConfigQuorum()->shards;

    FOREACH(it, shards)
    {
        if (*it == shardID)
            break;
    }
    assert(it != NULL);

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
