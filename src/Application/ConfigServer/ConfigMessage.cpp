#include "ConfigMessage.h"
#include "System/Buffers/Buffer.h"

bool ConfigMessage::SetClusterID(uint64_t clusterID_)
{
    type = CONFIGMESSAGE_SET_CLUSTER_ID;
    clusterID = clusterID_;
    
    return true;
}

bool ConfigMessage::RegisterShardServer(
 uint64_t nodeID_, Endpoint& endpoint_)
{
    type = CONFIGMESSAGE_REGISTER_SHARDSERVER;
    nodeID = nodeID_;
    endpoint = endpoint_;

    return true;
}

bool ConfigMessage::CreateQuorum(
 List<uint64_t>& nodes_)
{
    type = CONFIGMESSAGE_CREATE_QUORUM;
    nodes = nodes_;
    return true;
}

bool ConfigMessage::DeleteQuorum(
 uint64_t quorumID_)
{
    type = CONFIGMESSAGE_DELETE_QUORUM;
    quorumID = quorumID_;
    return true;
}

bool ConfigMessage::AddNode(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_ADD_NODE;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ConfigMessage::RemoveNode(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_REMOVE_NODE;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ConfigMessage::ActivateShardServer(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_ACTIVATE_SHARDSERVER;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ConfigMessage::DeactivateShardServer(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_DEACTIVATE_SHARDSERVER;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ConfigMessage::CreateDatabase(
 ReadBuffer& name_)
{
    type = CONFIGMESSAGE_CREATE_DATABASE;
    name = name_;
    return true;
}

bool ConfigMessage::RenameDatabase(
 uint64_t databaseID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_RENAME_DATABASE;
    databaseID = databaseID_;
    name = name_;
    return true;
}

bool ConfigMessage::DeleteDatabase(
 uint64_t databaseID_)
{
    type = CONFIGMESSAGE_DELETE_DATABASE;
    databaseID = databaseID_;
    return true;
}

bool ConfigMessage::CreateTable(
 uint64_t databaseID_, uint64_t quorumID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_CREATE_TABLE;
    databaseID = databaseID_;
    quorumID = quorumID_;
    name = name_;
    return true;
}

bool ConfigMessage::RenameTable(
 uint64_t tableID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_RENAME_TABLE;
    tableID = tableID_;
    name = name_;
    return true;
}

bool ConfigMessage::DeleteTable(
 uint64_t tableID_)
{
    type = CONFIGMESSAGE_DELETE_TABLE;
    tableID = tableID_;
    return true;
}

bool ConfigMessage::FreezeTable(
 uint64_t tableID_)
{
    type = CONFIGMESSAGE_FREEZE_TABLE;
    tableID = tableID_;
    return true;
}

bool ConfigMessage::UnfreezeTable(
 uint64_t tableID_)
{
    type = CONFIGMESSAGE_UNFREEZE_TABLE;
    tableID = tableID_;
    return true;
}

bool ConfigMessage::TruncateTableBegin(
 uint64_t tableID_)
{
    type = CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN;
    tableID = tableID_;
    return true;
}

bool ConfigMessage::TruncateTableComplete(
 uint64_t tableID_)
{
    type = CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE;
    tableID = tableID_;
    return true;
}

bool ConfigMessage::SplitShardBegin(uint64_t shardID_, ReadBuffer& splitKey_)
{
    type = CONFIGMESSAGE_SPLIT_SHARD_BEGIN;
    shardID = shardID_;
    splitKey.Write(splitKey_);
    return true;
}

bool ConfigMessage::SplitShardComplete(uint64_t shardID_)
{
    type = CONFIGMESSAGE_SPLIT_SHARD_COMPLETE;
    shardID = shardID_;
    return true;
}

bool ConfigMessage::ShardMigrationComplete(uint64_t quorumID_, uint64_t shardID_)
{
    type = CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE;
    quorumID = quorumID_;
    shardID = shardID_;
    return true;
}

bool ConfigMessage::Read(ReadBuffer& buffer)
{
    int         read;
    unsigned    numNodes, i;
    uint64_t    nodeID_;
    ReadBuffer  rb;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        // Cluster management
        case CONFIGMESSAGE_SET_CLUSTER_ID:
            read = buffer.Readf("%c:%U",
             &type, &clusterID);
             break;
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            read = buffer.Readf("%c:%#R",
             &type, &rb);
            endpoint.Set(rb);
            break;
        case CONFIGMESSAGE_CREATE_QUORUM:
            read = buffer.Readf("%c:%u",
             &type, &numNodes);
             if (read < 0 || read == (signed)buffer.GetLength())
                return false;
            buffer.Advance(read);
            for (i = 0; i < numNodes; i++)
            {
                read = buffer.Readf(":%U", &nodeID_);
                if (read < 0)
                    return false;
                buffer.Advance(read);
                nodes.Append(nodeID_);
            }
            if (buffer.GetLength() == 0)
                return true;
            else
                return false;
            break;
        case CONFIGMESSAGE_DELETE_QUORUM:
            read = buffer.Readf("%c:%U",
             &type, &quorumID);
            break;
        case CONFIGMESSAGE_ADD_NODE:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &nodeID);
            break;
        case CONFIGMESSAGE_REMOVE_NODE:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &nodeID);
            break;
        case CONFIGMESSAGE_ACTIVATE_SHARDSERVER:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &nodeID);
            break;
        case CONFIGMESSAGE_DEACTIVATE_SHARDSERVER:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &nodeID);
            break;

        // Database management
        case CONFIGMESSAGE_CREATE_DATABASE:
            read = buffer.Readf("%c:%#R",
             &type, &name);
            break;
        case CONFIGMESSAGE_RENAME_DATABASE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &databaseID, &name);
            break;
        case CONFIGMESSAGE_DELETE_DATABASE:
            read = buffer.Readf("%c:%U", 
            &type, &databaseID);
            break;

        // Table management
        case CONFIGMESSAGE_CREATE_TABLE:
            read = buffer.Readf("%c:%U:%U:%#R",
             &type, &databaseID, &quorumID, &name);
            break;
        case CONFIGMESSAGE_RENAME_TABLE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &tableID, &name);
            break;
        case CONFIGMESSAGE_DELETE_TABLE:
            read = buffer.Readf("%c:%U",
             &type, &tableID);
            break;
        case CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN:
            read = buffer.Readf("%c:%U",
             &type, &tableID);
            break;
        case CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE:
            read = buffer.Readf("%c:%U",
             &type, &tableID);
            break;
        case CONFIGMESSAGE_FREEZE_TABLE:
            read = buffer.Readf("%c:%U",
             &type, &tableID);
            break;
        case CONFIGMESSAGE_UNFREEZE_TABLE:
            read = buffer.Readf("%c:%U",
             &type, &tableID);
            break;

        // Shard management
        case CONFIGMESSAGE_SPLIT_SHARD_BEGIN:
            read = buffer.Readf("%c:%U:%#B",
             &type, &shardID, &splitKey);
            break;
        case CONFIGMESSAGE_SPLIT_SHARD_COMPLETE:
            read = buffer.Readf("%c:%U",
             &type, &shardID);
            break;
        case CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &shardID);
            break;

        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength() ? true : false);
}

bool ConfigMessage::Write(Buffer& buffer)
{
    ReadBuffer      rb;
    uint64_t*       it;
    unsigned        numNodes;
    
    switch (type)
    {
        // Cluster management
        case CONFIGMESSAGE_SET_CLUSTER_ID:
            buffer.Writef("%c:%U",
             type, clusterID);
            return true;
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            rb = endpoint.ToReadBuffer();
            buffer.Writef("%c:%#R",
             type, &rb);
            return true;
        case CONFIGMESSAGE_CREATE_QUORUM:
            numNodes = nodes.GetLength();
            buffer.Writef("%c:%u",
             type, numNodes);
            for (it = nodes.First(); it != NULL; it = nodes.Next(it))
                buffer.Appendf(":%U", *it);
            break;
        case CONFIGMESSAGE_DELETE_QUORUM:
            buffer.Writef("%c:%U",
             type, quorumID);
            break;
        case CONFIGMESSAGE_ADD_NODE:
            buffer.Writef("%c:%U:%U",
             type, quorumID, nodeID);
            break;
        case CONFIGMESSAGE_REMOVE_NODE:
            buffer.Writef("%c:%U:%U",
             type, quorumID, nodeID);
            break;
        case CONFIGMESSAGE_ACTIVATE_SHARDSERVER:
            buffer.Writef("%c:%U:%U",
             type, quorumID, nodeID);
            break;
        case CONFIGMESSAGE_DEACTIVATE_SHARDSERVER:
            buffer.Writef("%c:%U:%U",
             type, quorumID, nodeID);
            break;

        // Database management
        case CONFIGMESSAGE_CREATE_DATABASE:
            buffer.Writef("%c:%#R",
             type, &name);
            break;
        case CONFIGMESSAGE_RENAME_DATABASE:
            buffer.Writef("%c:%U:%#R",
             type, databaseID, &name);
            break;
        case CONFIGMESSAGE_DELETE_DATABASE:
            buffer.Writef("%c:%U",
             type, databaseID);
            break;

        // Table management
        case CONFIGMESSAGE_CREATE_TABLE:
            buffer.Writef("%c:%U:%U:%#R",
             type, databaseID, quorumID, &name);
            break;
        case CONFIGMESSAGE_RENAME_TABLE:
            buffer.Writef("%c:%U:%#R",
             type, tableID, &name);
            break;
        case CONFIGMESSAGE_DELETE_TABLE:
            buffer.Writef("%c:%U",
             type, tableID);
            break;
        case CONFIGMESSAGE_TRUNCATE_TABLE_BEGIN:
            buffer.Writef("%c:%U",
             type, tableID);
            break;
        case CONFIGMESSAGE_TRUNCATE_TABLE_COMPLETE:
            buffer.Writef("%c:%U",
             type, tableID);
            break;
        case CONFIGMESSAGE_FREEZE_TABLE:
            buffer.Writef("%c:%U",
             type, tableID);
            break;
        case CONFIGMESSAGE_UNFREEZE_TABLE:
            buffer.Writef("%c:%U",
             type, tableID);
            break;
            
        // Shard management
        case CONFIGMESSAGE_SPLIT_SHARD_BEGIN:
            buffer.Writef("%c:%U:%#B",
             type, shardID, &splitKey);
            break;
        case CONFIGMESSAGE_SPLIT_SHARD_COMPLETE:
            buffer.Writef("%c:%U",
             type, shardID);
             break;
        case CONFIGMESSAGE_SHARD_MIGRATION_COMPLETE:
            buffer.Writef("%c:%U:%U",
             type, quorumID, shardID);
            break;
        
        default:
            return false;
    }
    
    return true;
}
