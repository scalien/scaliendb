#ifndef PAXOSLEASEACCEPTORSTATE_H
#define PAXOSLEASEACCEPTORSTATE_H

#include "System/Platform.h"

/*
===============================================================================================

 PaxosLeaseAcceptorState

===============================================================================================
*/

struct PaxosLeaseAcceptorState
{
    void            Init();
    void            OnLeaseTimeout();

    uint64_t        promisedProposalID;
    bool            accepted;
    uint64_t        acceptedProposalID;
    uint64_t        acceptedLeaseOwner;
    unsigned        acceptedDuration;
    uint64_t        acceptedExpireTime;
};

/*
===============================================================================================
*/

inline void PaxosLeaseAcceptorState::Init()
{
    promisedProposalID = 0;

    accepted = false;
    acceptedProposalID  = 0;
    acceptedLeaseOwner  = 0;
    acceptedDuration    = 0;
    acceptedExpireTime  = 0;
}

inline void PaxosLeaseAcceptorState::OnLeaseTimeout()
{
    accepted = false;
    acceptedProposalID = 0;
    acceptedLeaseOwner = 0;
    acceptedDuration   = 0;
    acceptedExpireTime = 0;
}

#endif
