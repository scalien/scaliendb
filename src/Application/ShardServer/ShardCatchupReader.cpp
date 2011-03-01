#include "ShardCatchupReader.h"
#include "Application/ConfigState/ConfigState.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void ShardCatchupReader::Init(ShardQuorumProcessor* quorumProcessor_)
{
    quorumProcessor = quorumProcessor_;
    isActive = false;
    shardID = 0;
    
    environment = quorumProcessor->GetShardServer()->GetDatabaseManager()->GetEnvironment();
}

bool ShardCatchupReader::IsActive()
{
    return isActive;
}

void ShardCatchupReader::Begin()
{
    isActive = true;
    bytesReceived = 0;
    nextCommit = CATCHUP_COMMIT_GRANULARITY;
}

void ShardCatchupReader::Abort()
{
    isActive = false;
}

void ShardCatchupReader::OnBeginShard(CatchupMessage& msg)
{
    ConfigShard*        shard;
    ReadBuffer          firstKey;

    assert(isActive);

    shard = quorumProcessor->GetShardServer()->GetConfigState()->GetShard(msg.shardID);
    assert(shard != NULL);

    environment->Commit();
    environment->DeleteShard(QUORUM_DATABASE_DATA_CONTEXT, shard->shardID);
    environment->Commit();
    environment->CreateShard(QUORUM_DATABASE_DATA_CONTEXT, shard->shardID,
     shard->tableID, shard->firstKey, shard->lastKey, true, false);
     
     shardID = shard->shardID;
}

void ShardCatchupReader::OnSet(CatchupMessage& msg)
{
    environment->Set(QUORUM_DATABASE_DATA_CONTEXT, shardID, msg.key, msg.value);
    bytesReceived += msg.key.GetLength() + msg.value.GetLength();
    TryCommit();
}

void ShardCatchupReader::OnDelete(CatchupMessage& msg)
{
    environment->Delete(QUORUM_DATABASE_DATA_CONTEXT, shardID, msg.key);
    bytesReceived += msg.key.GetLength();
    TryCommit();
}

void ShardCatchupReader::OnCommit(CatchupMessage& message)
{
    assert(isActive);
    
    isActive = false;
    quorumProcessor->SetPaxosID(message.paxosID);

    Log_Message("Catchup complete");
}

void ShardCatchupReader::OnAbort(CatchupMessage& /*message*/)
{
    assert(isActive);
    
    isActive = false;

    Log_Message("Catchup aborted");
}

void ShardCatchupReader::TryCommit()
{
    if (bytesReceived >= nextCommit)
    {
        environment->Commit();
        nextCommit = bytesReceived + CATCHUP_COMMIT_GRANULARITY;
    }
}
