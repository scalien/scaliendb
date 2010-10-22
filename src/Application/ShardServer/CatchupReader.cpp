#include "CatchupReader.h"
#include "Framework/Storage/StorageTable.h"
#include "Application/ConfigState/ConfigState.h"
#include "ShardQuorumProcessor.h"
#include "ShardServer.h"

void CatchupReader::Init(ShardQuorumProcessor* quorumProcessor_)
{
    quorumProcessor = quorumProcessor_;
    isActive = false;
}

bool CatchupReader::IsActive()
{
    return isActive;
}

void CatchupReader::Begin()
{
    isActive = true;
}

void CatchupReader::Abort()
{
    isActive = false;
}

void CatchupReader::OnBeginShard(CatchupMessage& msg)
{
    ConfigShard*    cshard;
    StorageTable*   table;
    ReadBuffer      firstKey;

    if (!isActive)
        return;

    cshard = quorumProcessor->GetShardServer()->GetConfigState().GetShard(msg.shardID);
    if (!cshard)
    {
        ASSERT_FAIL();
        return;
    }
        
    table = quorumProcessor->GetShardServer()->GetDatabaseAdapter().GetTable(cshard->tableID);
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

void CatchupReader::OnKeyValue(CatchupMessage& msg)
{
    if (!isActive)
        return;
    
    assert(shard != NULL);

    shard->Set(msg.key, msg.value, true);
}

void CatchupReader::OnCommit(CatchupMessage& /*message*/)
{
    if (!isActive)
        return;

    assert(shard != NULL);
    
    shard = NULL;
    isActive = false;

    Log_Message("Catchup complete");
}

void CatchupReader::OnAbort(CatchupMessage& /*message*/)
{
    // TODO: xxx
}
