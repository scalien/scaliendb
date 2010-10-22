#include "ShardCatchupWriter.h"
#include "Application/Common/ContextTransport.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void ShardCatchupWriter::Init(ShardQuorumProcessor* quorumProcessor_)
{
    quorumProcessor = quorumProcessor_;
    Reset();
}

void ShardCatchupWriter::Reset()
{
    cursor = NULL;
    isActive = false;
}

bool ShardCatchupWriter::IsActive()
{
    return isActive;
}

void ShardCatchupWriter::Begin(CatchupMessage& request)
{    
    if (isActive)
        return;
        
    assert(quorumProcessor != NULL);

    Reset();
    isActive = true;
    nodeID = request.nodeID;
    quorumID = request.quorumID;

    paxosID = quorumProcessor->GetPaxosID() - 1;

    if (quorumProcessor->GetShards().GetLength() == 0)
    {
        Commit();
        return;
    }
    
    CONTEXT_TRANSPORT->RegisterWriteReadyness(nodeID, MFUNC(ShardCatchupWriter, OnWriteReadyness));
    isActive = true;
    
    SendFirst();
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
    StorageShard*       shard;
    StorageKeyValue*    kv;

    assert(quorumProcessor->GetShards().GetLength() > 0);
    shardID = *(quorumProcessor->GetShards().First());
    shard = quorumProcessor->GetShardServer()->GetDatabaseAdapter()->GetShard(shardID);
    assert(shard != NULL);

    assert(cursor == NULL);
    cursor = new StorageCursor(shard);

    msg.BeginShard(shardID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);

    // send first KV
    kv = cursor->Begin();
    if (!kv)
    {
        SendNext();
        return;
    }
    
    msg.KeyValue(kv->key, kv->value);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
}

void ShardCatchupWriter::SendNext()
{
    uint64_t*           itShardID;
    CatchupMessage      msg;
    StorageShard*       shard;
    StorageKeyValue*    kv;

    assert(cursor != NULL);
    kv = cursor->Next();
    if (kv)
    {
        msg.KeyValue(kv->key, kv->value);
        CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
        return;
    }

    // kv is NULL, at end of current shard
    delete cursor;
    cursor = NULL;
    
    itShardID = NextShard();
    
    if (!itShardID)
    {
        Commit(); // there is no next shard, send commit
        return;
    }    

    shardID = *itShardID;
    shard = quorumProcessor->GetShardServer()->GetDatabaseAdapter()->GetShard(shardID);
    assert(shard != NULL);

    msg.BeginShard(shardID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);

    // send first KV
    cursor = new StorageCursor(shard);
    kv = cursor->Begin();
    if (!kv)
        return;

    msg.KeyValue(kv->key, kv->value);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
}

void ShardCatchupWriter::Commit()
{
    CatchupMessage msg;
    
    msg.Commit(paxosID);
    CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);

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
    
    ShardQuorumProcessor::ShardList& shards = quorumProcessor->GetShards();

    FOREACH(it, shards)
    {
        if (*it == shardID)
            break;
    }
    assert(it != NULL);

    it = shards.Next(it);
    
    return it;
}
