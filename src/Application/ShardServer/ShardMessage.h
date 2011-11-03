#ifndef SHARDMESSAGE_H
#define SHARDMESSAGE_H

#include "Framework/Messaging/Message.h"

#define SHARDMESSAGE_SET                    'S'
#define SHARDMESSAGE_SET_IF_NOT_EXISTS      'I'
#define SHARDMESSAGE_TEST_AND_SET           's'
#define SHARDMESSAGE_TEST_AND_DELETE        'i'
#define SHARDMESSAGE_GET_AND_SET            'g'
#define SHARDMESSAGE_ADD                    'a'
#define SHARDMESSAGE_SEQUENCE_ADD           'A'
#define SHARDMESSAGE_APPEND                 'p'
#define SHARDMESSAGE_DELETE                 'X'
#define SHARDMESSAGE_REMOVE                 'x'
#define SHARDMESSAGE_SPLIT_SHARD            'z'
#define SHARDMESSAGE_TRUNCATE_TABLE         'y'
#define SHARDMESSAGE_MIGRATION_BEGIN        '1'
#define SHARDMESSAGE_MIGRATION_SET          '2'
#define SHARDMESSAGE_MIGRATION_DELETE       '3'
#define SHARDMESSAGE_MIGRATION_COMPLETE     '4'

class ClientRequest;

/*
===============================================================================================

 ShardMessage

===============================================================================================
*/

class ShardMessage
{
public:
    // Variables
    char            type;
    uint64_t        configPaxosID;
    uint64_t        tableID;
    uint64_t        shardID;
    uint64_t        newShardID;
    uint64_t        srcShardID;
    uint64_t        dstShardID;
    int64_t         number;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      test;
    Buffer          splitKey;
    Buffer          migrationKey;
    Buffer          migrationValue;
    ClientRequest*  clientRequest;

    // Constructor
    ShardMessage();
    
    bool            IsClientWrite();

    void            SplitShard(uint64_t shardID, uint64_t newShardID, ReadBuffer& splitKey);
    void            TruncateTable(uint64_t tableID, uint64_t newShardID);

    void            ShardMigrationBegin(uint64_t srcShardID, uint64_t dstShardID);
    void            ShardMigrationSet(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    void            ShardMigrationDelete(uint64_t shardID, ReadBuffer& key);
    void            ShardMigrationComplete(uint64_t shardID);
    
    // Serialization
    int             Read(ReadBuffer& buffer);
    bool            Append(Buffer& buffer);

    // For InList<>
    ShardMessage*   prev;
    ShardMessage*   next;
};

#endif
