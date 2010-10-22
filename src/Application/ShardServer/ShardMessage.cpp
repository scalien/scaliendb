#include "ShardMessage.h"

void ShardMessage::Set(uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = SHARDMESSAGE_SET;
    tableID = tableID_;
    key = key_;
    value = value_;
}

void ShardMessage::Delete(uint64_t tableID_, ReadBuffer& key_)
{
    type = SHARDMESSAGE_DELETE;
    tableID = tableID_;
    key = key_;
}

int ShardMessage::Read(ReadBuffer& buffer)
{
    int read;
    
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        // Data management
        case SHARDMESSAGE_SET:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case SHARDMESSAGE_DELETE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &tableID, &key);
            break;
        default:
            return false;
    }
    
    return read;
}

bool ShardMessage::Write(Buffer& buffer)
{
    switch (type)
    {
        // Cluster management
        case SHARDMESSAGE_SET:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case SHARDMESSAGE_DELETE:
            buffer.Appendf("%c:%U:%#R",
             type, tableID, &key);
            break;
        default:
            return false;
    }
    
    return true;
}
