#include "ShardMessage.h"

ShardMessage::ShardMessage()
{
    prev = next = this;
    clientRequest = NULL;
    configPaxosID = 0;
}

bool ShardMessage::IsClientWrite()
{
    return (type == SHARDMESSAGE_SET ||
            type == SHARDMESSAGE_SET_IF_NOT_EXISTS ||
            type == SHARDMESSAGE_TEST_AND_SET ||
            type == SHARDMESSAGE_TEST_AND_DELETE ||
            type == SHARDMESSAGE_GET_AND_SET ||
            type == SHARDMESSAGE_ADD ||
            type == SHARDMESSAGE_SEQUENCE_ADD ||
            type == SHARDMESSAGE_APPEND ||
            type == SHARDMESSAGE_DELETE ||
            type == SHARDMESSAGE_REMOVE);
}

void ShardMessage::SplitShard(uint64_t shardID_, uint64_t newShardID_, ReadBuffer& splitKey_)
{
    type = SHARDMESSAGE_SPLIT_SHARD;
    shardID = shardID_;
    newShardID = newShardID_;
    splitKey.Write(splitKey_);
}

void ShardMessage::TruncateTable(uint64_t tableID_, uint64_t newShardID_)
{
    type = SHARDMESSAGE_TRUNCATE_TABLE;
    tableID = tableID_;
    newShardID = newShardID_;
}

void ShardMessage::ShardMigrationBegin(uint64_t srcShardID_, uint64_t dstShardID_)
{
    type = SHARDMESSAGE_MIGRATION_BEGIN;
    srcShardID = srcShardID_;
    dstShardID = dstShardID_;
}

void ShardMessage::ShardMigrationSet(uint64_t shardID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = SHARDMESSAGE_MIGRATION_SET;
    shardID = shardID_;
    migrationKey.Write(key_);
    migrationValue.Write(value_);
    key.Wrap(migrationKey);
    value.Wrap(migrationValue);
}

void ShardMessage::ShardMigrationDelete(uint64_t shardID_, ReadBuffer& key_)
{
    type = SHARDMESSAGE_MIGRATION_DELETE;
    shardID = shardID_;
    migrationKey.Write(key_);
    key.Wrap(migrationKey);
}

void ShardMessage::ShardMigrationComplete(uint64_t shardID_)
{
    type = SHARDMESSAGE_MIGRATION_COMPLETE;
    shardID = shardID_;
}

int ShardMessage::Read(ReadBuffer& buffer)
{
    int read;
    
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        // Data manipulation
        case SHARDMESSAGE_SET:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case SHARDMESSAGE_SET_IF_NOT_EXISTS:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case SHARDMESSAGE_TEST_AND_SET:
            read = buffer.Readf("%c:%U:%#R:%#R:%#R",
             &type, &tableID, &key, &test, &value);
            break;
        case SHARDMESSAGE_TEST_AND_DELETE:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &test);
            break;
        case SHARDMESSAGE_GET_AND_SET:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case SHARDMESSAGE_ADD:
        case SHARDMESSAGE_SEQUENCE_ADD:
            read = buffer.Readf("%c:%U:%#R:%I",
             &type, &tableID, &key, &number);
            break;
        case SHARDMESSAGE_APPEND:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &tableID, &key, &value);
            break;
        case SHARDMESSAGE_DELETE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &tableID, &key);
            break;
        case SHARDMESSAGE_REMOVE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &tableID, &key);
            break;
        // Shard splitting
        case SHARDMESSAGE_SPLIT_SHARD:
            read = buffer.Readf("%c:%U:%U:%#B",
             &type, &shardID, &newShardID, &splitKey);
            break;
        // Shard manipulation
        case SHARDMESSAGE_TRUNCATE_TABLE:
            read = buffer.Readf("%c:%U:%U",
             &type, &tableID, &newShardID);
            break;
        case SHARDMESSAGE_MIGRATION_BEGIN:
            read = buffer.Readf("%c:%U:%U",
             &type, &srcShardID, &dstShardID);
            break;
        case SHARDMESSAGE_MIGRATION_SET:
            read = buffer.Readf("%c:%U:%#R:%#R",
             &type, &shardID, &key, &value);
            break;
        case SHARDMESSAGE_MIGRATION_DELETE:
            read = buffer.Readf("%c:%U:%#R",
             &type, &shardID, &key);
            break;
        case SHARDMESSAGE_MIGRATION_COMPLETE:
            read = buffer.Readf("%c:%U",
             &type, &shardID);
            break;
        default:
            return false;
    }
    
    return read;
}

bool ShardMessage::Append(Buffer& buffer)
{
    switch (type)
    {
        // Data manipulation
        case SHARDMESSAGE_SET:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case SHARDMESSAGE_SET_IF_NOT_EXISTS:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case SHARDMESSAGE_TEST_AND_SET:
            buffer.Appendf("%c:%U:%#R:%#R:%#R",
             type, tableID, &key, &test, &value);
            break;
        case SHARDMESSAGE_TEST_AND_DELETE:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &test);
            break;
        case SHARDMESSAGE_GET_AND_SET:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case SHARDMESSAGE_ADD:
        case SHARDMESSAGE_SEQUENCE_ADD:
            buffer.Appendf("%c:%U:%#R:%I",
             type, tableID, &key, number);
            break;
        case SHARDMESSAGE_APPEND:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, tableID, &key, &value);
            break;
        case SHARDMESSAGE_DELETE:
            buffer.Appendf("%c:%U:%#R",
             type, tableID, &key);
            break;
        case SHARDMESSAGE_REMOVE:
            buffer.Appendf("%c:%U:%#R",
             type, tableID, &key);
            break;
        // Shard splitting
        case SHARDMESSAGE_SPLIT_SHARD:
            buffer.Appendf("%c:%U:%U:%#B",
             type, shardID, newShardID, &splitKey);
            break;
        // Shard migration
        case SHARDMESSAGE_TRUNCATE_TABLE:
            buffer.Appendf("%c:%U:%U",
             type, tableID, newShardID);
            break;
        case SHARDMESSAGE_MIGRATION_BEGIN:
            buffer.Appendf("%c:%U:%U",
             type, srcShardID, dstShardID);
            break;
        case SHARDMESSAGE_MIGRATION_SET:
            buffer.Appendf("%c:%U:%#R:%#R",
             type, shardID, &key, &value);
            break;
        case SHARDMESSAGE_MIGRATION_DELETE:
            buffer.Appendf("%c:%U:%#R",
             type, shardID, &key);
            break;
        case SHARDMESSAGE_MIGRATION_COMPLETE:
            buffer.Appendf("%c:%U",
             type, shardID);
            break;
        default:
            return false;
    }
    
    return true;
}
