#ifndef PAXOSACCEPTOR_H
#define PAXOSACCEPTOR_H

#include "System/Common.h"
#include "PaxosMessage.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "States/PaxosAcceptorState.h"

class ReplicatedLog; // forward

/*
===============================================================================

 PaxosAcceptor

===============================================================================
*/

class PaxosAcceptor
{
public:
    void                        Init(QuorumContext* context);
    void                        OnMessage(PaxosMessage& msg);

private:
    void                        OnPrepareRequest(PaxosMessage& msg);
    void                        OnProposeRequest(PaxosMessage& msg);
    void                        OnStateWritten();

    void                        ReadState();
    void                        WriteState();

    QuorumContext*              context;
    PaxosAcceptorState          state;
    PaxosMessage                omsg;
    uint64_t                    senderID;
    uint64_t                    writtenPaxosID;
    
    friend class ReplicatedLog;
};

#endif
