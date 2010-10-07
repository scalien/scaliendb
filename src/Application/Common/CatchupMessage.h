#ifndef CATCHUPMESSAGE_H
#define CATCHUPMESSAGE_H

#include "Framework/Messaging/Message.h"

#define CATCHUP_PROTOCOL_ID     'C'

#define CATCHUP_REQUEST         '1'
#define CATCHUP_RESPONSE        '2'
#define CATCHUP_COMMIT          '3'


class CatchupMessage : public Message
{
public:
    char            type;
    uint64_t        nodeID;
    uint64_t        paxosID;
    
    bool            CatchupRequest(uint64_t nodeID);
    bool            CatchupResponse(ReadBuffer& key, ReadBuffer& value);
    bool            Commit(uint64_t paxosID);

    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
