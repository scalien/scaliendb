#include "ShardCatchupReader.h"
#include "Application/ConfigState/ConfigState.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void ShardCatchupReader::Init(ShardQuorumProcessor* quorumProcessor_)
{
    quorumProcessor = quorumProcessor_;
    isActive = false;
}

bool ShardCatchupReader::IsActive()
{
    return isActive;
}

void ShardCatchupReader::Begin()
{
    isActive = true;
}

void ShardCatchupReader::Abort()
{
    isActive = false;
}

void ShardCatchupReader::OnBeginShard(CatchupMessage& msg)
{
    ConfigShard*    cshard;
    StorageTable*   table;
    ReadBuffer      firstKey;

    if (!isActive)
        return;

    cshard = quorumProcessor->GetShardServer()->GetConfigState()->GetShard(msg.shardID);
    if (!cshard)
    {
        ASSERT_FAIL();
        return;
    }
        
    table = quorumProcessor->GetShardServer()->GetDatabaseManager()->GetTable(cshard->tableID);
    if (!table)
    {
        ASSERT_FAIL();
        return;
    }
    
    shard = table->GetShard(msg.shardID);
    if (shard != NULL)
        table->DeleteShard(msg.shardID);
    
    // shard is either deleted or not yet created, therefore create it on disk
    firstKey.Wrap(cshard->firstKey);
    table->CreateShard(msg.shardID, firstKey);
    shard = table->GetShard(msg.shardID);
}

void ShardCatchupReader::OnKeyValue(CatchupMessage& msg)
{
    if (!isActive)
        return;
    
    assert(shard != NULL);

    shard->Set(msg.key, msg.value, true);
}

void ShardCatchupReader::OnCommit(CatchupMessage& /*message*/)
{
    if (!isActive)
        return;

    assert(shard != NULL);
    
    shard = NULL;
    isActive = false;

    Log_Message("Catchup complete");
}

void ShardCatchupReader::OnAbort(CatchupMessage& /*message*/)
{
    shard = NULL;
    isActive = false;

    Log_Message("Catchup aborted");
}
