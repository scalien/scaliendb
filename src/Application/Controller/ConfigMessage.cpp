#include "ConfigMessage.h"
#include "System/Buffers/Buffer.h"

bool ConfigMessage::RegisterShardServer(
 uint64_t nodeID_, Endpoint& endpoint_)
{
    type = CONFIGMESSAGE_REGISTER_SHARDSERVER;
    nodeID = nodeID_;
    endpoint = endpoint_;

    return true;
}

bool ConfigMessage::CreateQuorum(
 uint64_t quorumID_, NodeList& nodes_)
{
    type = CONFIGMESSAGE_CREATE_QUORUM;
    quorumID = quorumID_;
    nodes = nodes_;
    return true;
}

bool ConfigMessage::IncreaseQuorum(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_INCREASE_QUORUM;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ConfigMessage::DecreaseQuorum(
 uint64_t quorumID_, uint64_t nodeID_)
{
    type = CONFIGMESSAGE_DECREASE_QUORUM;
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
 uint64_t databaseID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_CREATE_DATABASE;
    databaseID = databaseID_;
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
 uint64_t databaseID_, uint64_t tableID_, uint64_t shardID_,
 uint64_t quorumID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_CREATE_TABLE;
    databaseID = databaseID_;
    tableID = tableID_;
    shardID = shardID_;
    quorumID = quorumID_;
    name = name_;
    return true;
}

bool ConfigMessage::RenameTable(
 uint64_t databaseID_, uint64_t tableID_, ReadBuffer& name_)
{
    type = CONFIGMESSAGE_RENAME_TABLE;
    databaseID = databaseID_;
    tableID = tableID_;
    name = name_;
    return true;
}

bool ConfigMessage::DeleteTable(
 uint64_t databaseID_, uint64_t tableID_)
{
    type = CONFIGMESSAGE_DELETE_TABLE;
    databaseID = databaseID_;
    tableID = tableID_;
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
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            read = buffer.Readf("%c:%U:%#R",
             &type, &nodeID, &rb);
            endpoint.Set(rb);
            break;
        case CONFIGMESSAGE_CREATE_QUORUM:
            read = buffer.Readf("%c:%U:%u",
             &type, &quorumID, &numNodes);
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
        case CONFIGMESSAGE_INCREASE_QUORUM:
            read = buffer.Readf("%c:%U:%U",
             &type, &quorumID, &nodeID);
            break;
        case CONFIGMESSAGE_DECREASE_QUORUM:
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
            read = buffer.Readf("%c:%U:%#R",
             &type, &databaseID, &name);
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
            read = buffer.Readf("%c:%U:%U:%U:%U:%#R",
             &type, &databaseID, &tableID, &shardID, &quorumID, &name);
            break;
        case CONFIGMESSAGE_RENAME_TABLE:
            read = buffer.Readf("%c:%U:%U:%#R",
             &type, &databaseID, &tableID, &name);
            break;
        case CONFIGMESSAGE_DELETE_TABLE:
            read = buffer.Readf("%c:%U:%U",
             &type, &databaseID, &tableID);
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
        case CONFIGMESSAGE_REGISTER_SHARDSERVER:
            rb = endpoint.ToReadBuffer();
            buffer.Writef("%c:%U:%#R",
             type, nodeID, &rb);
            return true;
        case CONFIGMESSAGE_CREATE_QUORUM:
            numNodes = nodes.GetLength();
            buffer.Writef("%c:%U:%u",
             type, quorumID, numNodes);
            for (it = nodes.First(); it != NULL; it = nodes.Next(it))
                buffer.Appendf(":%U", *it);
            break;
        case CONFIGMESSAGE_INCREASE_QUORUM:
            buffer.Writef("%c:%U:%U",
             type, quorumID, nodeID);
            break;
        case CONFIGMESSAGE_DECREASE_QUORUM:
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
            buffer.Writef("%c:%U:%#R",
             type, databaseID, &name);
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
            buffer.Writef("%c:%U:%U:%U:%U:%#R",
             type, databaseID, tableID, shardID, quorumID, &name);
            break;
        case CONFIGMESSAGE_RENAME_TABLE:
            buffer.Writef("%c:%U:%U:%#R",
             type, databaseID, tableID, &name);
            break;
        case CONFIGMESSAGE_DELETE_TABLE:
            buffer.Writef("%c:%U:%U",
             type, databaseID, tableID);
            break;
        default:
            return false;
    }
    
    return true;
}
