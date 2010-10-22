#ifndef CATCHUPMESSAGE_H
#define CATCHUPMESSAGE_H

#include "Framework/Messaging/Message.h"

#define CATCHUP_PROTOCOL_ID            'C'

#define CATCHUPMESSAGE_REQUEST         'R'
#define CATCHUPMESSAGE_BEGIN_SHARD     'B'
#define CATCHUPMESSAGE_KEYVALUE        'K'
#define CATCHUPMESSAGE_COMMIT          'C'
#define CATCHUPMESSAGE_ABORT           'A'
/*
===============================================================================================

 CatchupMessage

===============================================================================================
*/

class CatchupMessage : public Message
{
public:
    char            type;
    uint64_t        nodeID;
    uint64_t        paxosID;
    uint64_t        quorumID;
    uint64_t        shardID;
    ReadBuffer      key;
    ReadBuffer      value;
    
    bool            CatchupRequest(uint64_t nodeID, uint64_t quorumID);
    bool            BeginShard(uint64_t shardID);
    bool            KeyValue(ReadBuffer& key, ReadBuffer& value);
    bool            Commit(uint64_t paxosID);
    bool            Abort();

    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
