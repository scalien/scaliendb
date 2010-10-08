#include "CatchupMessage.h"

bool CatchupMessage::CatchupRequest(uint64_t nodeID_)
{
    type = CATCHUPMESSAGE_REQUEST;
    nodeID = nodeID_;
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

bool CatchupMessage::Read(ReadBuffer& buffer)
{
    int             read;

    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CATCHUPMESSAGE_REQUEST:
            read = buffer.Readf("%c:%U",
             &type, &nodeID);
            break;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            read = buffer.Readf("%c:%U",
             &type, &shardID);
            break;
        case CATCHUPMESSAGE_KEYVALUE:
            read = buffer.Readf("%c:%#R:%#R",
             &type, &key, &value);
            break;
        case CATCHUPMESSAGE_COMMIT:
            read = buffer.Readf("%c:%U",
             &type, &paxosID);
            break;
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength());
}

bool CatchupMessage::Write(Buffer& buffer)
{
    switch (type)
    {
        case CATCHUPMESSAGE_REQUEST:
            buffer.Writef("%c:%U",
             type, nodeID);
            return true;
        case CATCHUPMESSAGE_BEGIN_SHARD:
            buffer.Writef("%c:%U",
             type, shardID);
            return true;
        case CATCHUPMESSAGE_KEYVALUE:
            buffer.Writef("%c:%#R:%#R",
             type, &key, &value);
            return true;
        case CATCHUPMESSAGE_COMMIT:
            buffer.Writef("%c:%U",
             type, paxosID);
            return true;
        default:
            return false;
    }
    
    return true;
}
