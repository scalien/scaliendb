#include "ShardMigrationWriter.h"
#include "Application/Common/ContextTransport.h"
#include "ShardServer.h"

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
}

bool ShardMigrationWriter::IsActive()
{
    return isActive;
}

void ShardMigrationWriter::Begin(ClusterMessage& request)
{
    ConfigState*        configState;
    ConfigShardServer*  configShardServer;
    
    assert(!isActive);
    assert(cursor == NULL);

    configState = shardServer->GetConfigState();
    configShardServer = configState->GetShardServer(request.nodeID);
    assert(configShardServer != NULL);
    
    isActive = true;
    nodeID = request.nodeID;
    quorumID = request.quorumID;
    shardID = request.shardID;
    
    CONTEXT_TRANSPORT->AddNode(nodeID, configShardServer->endpoint);
    
    Log_Debug("ShardMigrationWriter::Begin() nodeID = %U", nodeID);
    Log_Debug("ShardMigrationWriter::Begin() quorumID = %U", quorumID);
    Log_Debug("ShardMigrationWriter::Begin() shardID = %U", shardID);

    Log_Message("Migrating shard %U into quorum %U (sending)", shardID, quorumID);

    CONTEXT_TRANSPORT->RegisterWriteReadyness(nodeID, MFUNC(ShardMigrationWriter, OnWriteReadyness));
    sendFirst = true;
}

void ShardMigrationWriter::Abort()
{
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

    Log_Debug("ShardMigrationWriter sedning ITEM");
    
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        msg.ShardMigrationSet(shardID, kv->GetKey(), kv->GetValue());
    else
        msg.ShardMigrationDelete(shardID, kv->GetKey());

    CONTEXT_TRANSPORT->SendClusterMessage(nodeID, msg);
}

void ShardMigrationWriter::OnWriteReadyness()
{
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
