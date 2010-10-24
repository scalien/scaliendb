#ifndef SHARDMESSAGE_H
#define SHARDMESSAGE_H

#include "Framework/Messaging/Message.h"

#define SHARDMESSAGE_SET                    'S'
#define SHARDMESSAGE_SET_IF_NOT_EXISTS      'I'
#define SHARDMESSAGE_TEST_AND_SET           's'
#define SHARDMESSAGE_DELETE                 'D'

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
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      test;

    // Data manipulation
    void            Set(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            SetIfNotExists(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            TestAndSet(uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    void            Delete(uint64_t tableID, ReadBuffer& key);

    // For InList<>
    ShardMessage*    prev;
    ShardMessage*    next;

    // Serialization
    int             Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
