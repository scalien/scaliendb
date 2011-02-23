#include "ShardMigrationWriter.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"
#include "ShardQuorumProcessor.h"

ShardMigrationWriter::ShardMigrationWriter()
{
    Reset();
}

ShardMigrationWriter::~ShardMigrationWriter()
{
    if (cursor != NULL)
        delete cursor;
}

void ShardMigrationWriter::Init(ShardServer* shardServer_)
{
    shardServer = shardServer_;
    environment = shardServer->GetDatabaseManager()->GetEnvironment();

    Reset();
}

void ShardMigrationWriter::Reset()
{
    cursor = NULL;
    isActive = false;
    sendFirst = false;
    quorumProcessor = NULL;
}

bool ShardMigrationWriter::IsActive()
{
    return isActive;
}

void ShardMigrationWriter::Begin(ClusterMessage& request)
{
    ConfigState*        configState;
    ConfigShard*        configShard;
    ConfigShardServer*  configShardServer;
    
    assert(!isActive);
    assert(cursor == NULL);

    configState = shardServer->GetConfigState();
    configShard = configState->GetShard(request.shardID);
    assert(configShard != NULL);
    configShardServer = configState->GetShardServer(request.nodeID);
    assert(configShardServer != NULL);
    
    quorumProcessor = shardServer->GetQuorumProcessor(configShard->quorumID);
    assert(quorumProcessor != NULL);
    
    isActive = true;
    nodeID = request.nodeID;
    quorumID = request.quorumID;
    shardID = request.shardID;
    
    CONTEXT_TRANSPORT->AddNode(nodeID, configShardServer->endpoint);
    
    Log_Debug("ShardMigrationWriter::Begin() nodeID = %U", nodeID);
    Log_Debug("ShardMigrationWriter::Begin() quorumID = %U", quorumID);
    Log_Debug("ShardMigrationWriter::Begin() shardID = %U", shardID);

    Log_Message("Migrating shard %U into quorum %U (sending)", shardID, quorumID);

    sendFirst = true;
    CONTEXT_TRANSPORT->RegisterWriteReadyness(nodeID, MFUNC(ShardMigrationWriter, OnWriteReadyness));
}

void ShardMigrationWriter::Abort()
{
    Log_Message("Aborting shard migration...", shardID, quorumID);
    
    CONTEXT_TRANSPORT->UnregisterWriteReadyness(nodeID, MFUNC(ShardMigrationWriter, OnWriteReadyness));
    Reset();

    if (cursor != NULL)
    {
        delete cursor;
        cursor = NULL;
    }
}

void ShardMigrationWriter::SendFirst()
{
    ClusterMessage      msg;
    ReadBuffer          key;
    ReadBuffer          value;

    assert(cursor == NULL);
    cursor = environment->GetBulkCursor(QUORUM_DATABASE_DATA_CONTEXT, shardID);

    msg.ShardMigrationBegin(quorumID, shardID);
    CONTEXT_TRANSPORT->SendClusterMessage(nodeID, msg);

    Log_Debug("ShardMigrationWriter sending BEGIN");

    // send first KV
    kv = cursor->First();
    if (kv)
        SendItem(kv);
    else
        SendCommit();
}

void ShardMigrationWriter::SendNext()
{
    assert(cursor != NULL);
    kv = cursor->Next(kv);
    if (kv)
        SendItem(kv);
    else
        SendCommit();
}

void ShardMigrationWriter::SendCommit()
{
    ClusterMessage msg;
    
    msg.ShardMigrationCommit(quorumID, shardID);
    CONTEXT_TRANSPORT->SendClusterMessage(nodeID, msg);

    Log_Message("Finished sending shard %U...", shardID);

    if (cursor != NULL)
    {
        delete cursor;
        cursor = NULL;
    }

    CONTEXT_TRANSPORT->UnregisterWriteReadyness(nodeID, MFUNC(ShardMigrationWriter, OnWriteReadyness));

    Reset();
}

void ShardMigrationWriter::SendItem(StorageKeyValue* kv)
{
    ClusterMessage msg;
    
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        msg.ShardMigrationSet(shardID, kv->GetKey(), kv->GetValue());
    else
        msg.ShardMigrationDelete(shardID, kv->GetKey());

    CONTEXT_TRANSPORT->SendClusterMessage(nodeID, msg);
}

void ShardMigrationWriter::OnWriteReadyness()
{
    Log_Debug();
    assert(quorumProcessor != NULL);
    if (!quorumProcessor->IsPrimary()
     || !shardServer->GetConfigState()->isMigrating
     || (shardServer->GetConfigState()->isMigrating &&
         (shardServer->GetConfigState()->migrateShardID != shardID ||
          shardServer->GetConfigState()->migrateQuorumID != quorumID)))
    {
        Abort();
        return;
    }
    
    if (sendFirst)
    {
        sendFirst = false;
        SendFirst();
    }
    else
    {
        SendNext();
    }
}
