#ifndef PAXOSLEASEPROPOSER_H
#define PAXOSLEASEPROPOSER_H

#include "System/Common.h"
#include "System/Events/Countdown.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosLeaseMessage.h"
#include "States/PaxosLeaseProposerState.h"

/*
===============================================================================

 PaxosLeaseProposer

===============================================================================
*/

class PaxosLeaseProposer
{
public:
    void                        Init(QuorumContext* context);
    
    void                        OnMessage(PaxosLeaseMessage& msg);

    void                        StartAcquireLease();
    void                        StopAcquireLease();
    uint64_t                    GetHighestProposalID();
    void                        SetHighestProposalID(uint64_t highestProposalID);

private:
    void                        OnAcquireLeaseTimeout();
    void                        OnExtendLeaseTimeout();
    void                        OnPrepareResponse(PaxosLeaseMessage& msg);
    void                        OnProposeResponse(PaxosLeaseMessage& msg);

    void                        BroadcastMessage(PaxosLeaseMessage& msg);
    void                        StartPreparing();
    void                        StartProposing();
    void                        NewVote();

    QuorumContext*              context;
    QuorumVote*                 vote;
    PaxosLeaseProposerState     state;
    Timer                       extendLeaseTimeout;
    Countdown                   acquireLeaseTimeout;
    uint64_t                    highestProposalID;  
};

#endif
