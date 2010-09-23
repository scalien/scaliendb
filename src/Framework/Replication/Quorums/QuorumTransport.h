#ifndef QUORUMTRANSPORT_H
#define QUORUMTRANSPORT_H

#include "Quorum.h"
#include "Framework/Messaging/Message.h"

/*
===============================================================================

 QuorumTransport

===============================================================================
*/

class QuorumTransport
{
public:
    void                    SetQuorum(Quorum* quorum);
    void                    SetContextID(uint64_t contextID);
    
    void                    SendMessage(uint64_t nodeID, Message& msg, bool priority = false);
    void                    BroadcastMessage(Message& msg, bool priority = false);

private:
    uint64_t                contextID;
    Buffer                  prefix;
    Quorum*                 quorum;
};

#endif
