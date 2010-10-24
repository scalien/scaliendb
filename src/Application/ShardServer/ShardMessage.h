#ifndef SHARDMESSAGE_H
#define SHARDMESSAGE_H

#include "Framework/Messaging/Message.h"

#define SHARDMESSAGE_SET                    'S'
#define SHARDMESSAGE_SET_IF_NOT_EXISTS      'I'
#define SHARDMESSAGE_TEST_AND_SET           's'
#define SHARDMESSAGE_ADD                    'a'
#define SHARDMESSAGE_DELETE                 'X'
#define SHARDMESSAGE_REMOVE                 'x'

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
    uint64_t        tableID;
    int64_t         number;
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      test;

    // Data manipulation
    void            Set(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            SetIfNotExists(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            TestAndSet(uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    void            Add(uint64_t tableID, ReadBuffer& key, int64_t number);
    void            Delete(uint64_t tableID, ReadBuffer& key);
    void            Remove(uint64_t tableID, ReadBuffer& key);

    // Serialization
    int             Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);

    // For InList<>
    ShardMessage*   prev;
    ShardMessage*   next;
};

#endif
