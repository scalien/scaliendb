#include "StorageLogSectionSerializer.h"
#include "StorageLogConsts.h"

StorageLogSectionSerializer::StorageLogSectionSerializer()
{
    prevContextID = 0;
    prevShardID = 0;
}

void StorageLogSectionSerializer::Init()
{
    buffer.Clear();
    buffer.Allocate(STORAGE_LOGSECTION_HEAD_SIZE);
    buffer.Zero();
    buffer.SetLength(STORAGE_LOGSECTION_HEAD_SIZE);
}

Buffer& StorageLogSectionSerializer::GetBuffer()
{
    return buffer;
}

void StorageLogSectionSerializer::SerializeSet(
 uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value)
{
    ASSERT(key.GetLength() > 0);

    SerializePrefix(STORAGE_LOGSEGMENT_COMMAND_SET, contextID, shardID);
    
    // key
    buffer.AppendLittle16(key.GetLength());
    buffer.Append(key);
    
    // value
    buffer.AppendLittle32(value.GetLength());
    buffer.Append(value);
    
    Remember(contextID, shardID);
}

void StorageLogSectionSerializer::SerializeDelete(
 uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    ASSERT(key.GetLength() > 0);

    SerializePrefix(STORAGE_LOGSEGMENT_COMMAND_DELETE, contextID, shardID);
    
    // key
    buffer.AppendLittle16(key.GetLength());
    buffer.Append(key);
    
    Remember(contextID, shardID);
}

void StorageLogSectionSerializer::Finalize()
{
    uint64_t length;
    uint32_t checksum;
    
    length = buffer.GetLength();
    
    ASSERT(length >= STORAGE_LOGSECTION_HEAD_SIZE);

    if (length == STORAGE_LOGSECTION_HEAD_SIZE)
        return; // empty round

    // for future use: we do not calculate a checksum currently
    checksum = 0;

    // write section header
    buffer.SetLength(0);
    buffer.AppendLittle64(length);
    buffer.AppendLittle64(length - STORAGE_LOGSECTION_HEAD_SIZE);
    buffer.AppendLittle32(checksum);
    buffer.SetLength(length);
}

void StorageLogSectionSerializer::SerializePrefix(
 char command, uint16_t contextID, uint64_t shardID)
{
    // type
    buffer.Appendf("%c", command);

    // contextID shardID
    if (contextID == prevContextID && shardID == prevShardID)
    {
        buffer.Appendf("%b", true); // use previous shardID
    }
    else
    {
        buffer.Appendf("%b", false);
        buffer.AppendLittle16(contextID);
        buffer.AppendLittle64(shardID);
    }
}

void StorageLogSectionSerializer::Remember(uint16_t contextID, uint64_t shardID)
{
    prevContextID = contextID;
    prevShardID = shardID;
}

