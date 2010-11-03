#include "CatchupMessage.h"

bool CatchupMessage::CatchupRequest(uint64_t nodeID_, uint64_t quorumID_)
{
    type = CATCHUPMESSAGE_REQUEST;
    nodeID = nodeID_;
    quorumID = quorumID_;
    return true;
}

bool CatchupMessage::BeginShard(uint64_t shardID_)
{
    type = CATCHUPMESSAGE_BEGIN_SHARD;
    shardID = shardID_;
    return true;
}

bool CatchupMessage::KeyValue(ReadBuffer& key_, ReadBuffer& value_)
{
    type = CATCHUPMESSAGE_KEYVALUE;
    key = key_;
    value = value_;
    return true;
}

bool CatchupMessage::Commit(uint64_t paxosID_)
{
    type = CATCHUPMESSAGE_COMMIT;
    paxosID = paxosID_;
    return true;
}

bool CatchupMessage::Abort()
{
    type = CATCHUPMESSAGE_ABORT;
    return true;
}

bool CatchupMessage::Read(ReadBuffer& buffer)
{
    int     read;
    char    proto;

    if (buffer.GetLength() < 3)
        return false;
    
    switch (buffer.GetCharAt(2))
    {
        case CATCHUPMESSAGE_REQUEST:
            read = buffer.Readf("%c:%c:%U:%U",
             &proto, &type, &nodeID, &quorumID);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            read = buffer.Readf("%c:%c:%U",
             &proto, &type, &shardID);
            break;
        case CATCHUPMESSAGE_KEYVALUE:
            read = buffer.Readf("%c:%c:%#R:%#R",
             &proto, &type, &key, &value);
            break;
        case CATCHUPMESSAGE_COMMIT:
            read = buffer.Readf("%c:%c:%U",
             &proto, &type, &paxosID);
            break;
        case CATCHUPMESSAGE_ABORT:
            read = buffer.Readf("%c:%c",
             &proto, &type);
            break;
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
}

bool CatchupMessage::Write(Buffer& buffer)
{
    const char proto = CATCHUP_PROTOCOL_ID;

    switch (type)
    {
        case CATCHUPMESSAGE_REQUEST:
            buffer.Writef("%c:%c:%U:%U",
             proto, type, nodeID, quorumID);
            return true;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            buffer.Writef("%c:%c:%U",
             proto, type, shardID);
            return true;
        case CATCHUPMESSAGE_KEYVALUE:
            buffer.Writef("%c:%c:%#R:%#R",
             proto, type, &key, &value);
            return true;
        case CATCHUPMESSAGE_COMMIT:
            buffer.Writef("%c:%c",
             proto, type);
            return true;
        default:
            return false;
    }
}
