#include "DataMessage.h"

void DataMessage::Set(uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = DATAMESSAGE_SET;
    tableID = tableID_;
    key = key_;
    value = value_;
}

void DataMessage::Delete(uint64_t tableID_, ReadBuffer& key_)
{
    type = DATAMESSAGE_DELETE;
    tableID = tableID_;
    key = key_;
}

int DataMessage::Read(ReadBuffer& buffer)
{
    int read;
    
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        // Data management
        case DATAMESSAGE_SET:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case DATAMESSAGE_DELETE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &tableID, &key);
            break;
        default:
            return false;
    }
    
    return read;
}

bool DataMessage::Write(Buffer& buffer)
{
    switch (type)
    {
        // Cluster management
        case DATAMESSAGE_SET:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case DATAMESSAGE_DELETE:
            buffer.Appendf("%c:%U:%#R",
             type, tableID, &key);
            break;
        default:
            return false;
    }
    
    return true;
}
