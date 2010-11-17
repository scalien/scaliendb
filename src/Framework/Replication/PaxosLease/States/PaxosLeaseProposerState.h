#ifndef PAXOSLEASEPROPOSERSTATE_H
#define PAXOSLEASEPROPOSERSTATE_H

#include "System/Platform.h"

/*
===============================================================================================

 PaxosLeaseProposerState

===============================================================================================
*/

struct PaxosLeaseProposerState
{
    void            Init();
    bool            Active();   
    
    bool            preparing;
    bool            proposing;
    uint64_t        proposalID;
    uint64_t        highestReceivedProposalID;
    uint64_t        leaseOwner;
    unsigned        duration;
    uint64_t        expireTime;

};

/*
===============================================================================================
*/

inline void PaxosLeaseProposerState::Init()
{
    preparing = false;
    proposing = false;
    proposalID = 0;
    highestReceivedProposalID = 0;
    duration = 0;
    expireTime = 0;
}

inline bool PaxosLeaseProposerState::Active()
{
    return (preparing || proposing);
}

#endif
