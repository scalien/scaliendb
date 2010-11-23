#ifndef SHARDMESSAGE_H
#define SHARDMESSAGE_H

#include "Framework/Messaging/Message.h"

#define SHARDMESSAGE_SET                    'S'
#define SHARDMESSAGE_SET_IF_NOT_EXISTS      'I'
#define SHARDMESSAGE_TEST_AND_SET           's'
#define SHARDMESSAGE_GET_AND_SET            'g'
#define SHARDMESSAGE_ADD                    'a'
#define SHARDMESSAGE_APPEND                 'p'
#define SHARDMESSAGE_DELETE                 'X'
#define SHARDMESSAGE_REMOVE                 'x'
#define SHARDMESSAGE_SPLIT                  'z'

/*
===============================================================================================

 ShardMessage

===============================================================================================
*/

class ShardMessage
{
public:
    // Variables
    bool            fromClient;

    char            type;
    uint64_t        tableID;
    uint64_t        shardID;
    uint64_t        newShardID;
    int64_t         number;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      test;

    // Constructor
    ShardMessage();

    // Data manipulation
    void            Set(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            SetIfNotExists(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            TestAndSet(uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    void            GetAndSet(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            Add(uint64_t tableID, ReadBuffer& key, int64_t number);
    void            Append(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            Delete(uint64_t tableID, ReadBuffer& key);
    void            Remove(uint64_t tableID, ReadBuffer& key);
    void            Split(uint64_t shardID, uint64_t newShardID, ReadBuffer& key);

    // Serialization
    int             Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);

    // For InList<>
    ShardMessage*   prev;
    ShardMessage*   next;
};

#endif
