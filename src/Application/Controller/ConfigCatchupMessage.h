#ifndef CONFIGCATCHUPMESSAGE_H
#define CONFIGCATCHUPMESSAGE_H

#include "Framework/Messaging/Message.h"
#include "Application/Common/State/ConfigState.h"

#define CATCHUP_PROTOCOL_ID     'C'

class ConfigCatchupMessage : public Message
{
public:
    char            type;
    uint64_t        nodeID;
    uint64_t        paxosID;
    ConfigState     configState;
    
    bool            CatchupRequest(uint64_t nodeID);
    bool            CatchupResponse(uint64_t paxosID, ConfigState& configState);

    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
