#ifndef PAXOSACCEPTORSTATE_H
#define PAXOSACCEPTORSTATE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================================

 PaxosAcceptorState

===============================================================================================
*/

struct PaxosAcceptorState
{
    void            Init();
    void            OnNewPaxosRound();

    uint64_t        promisedProposalID;
    bool            accepted;   
    uint64_t        acceptedProposalID;
    uint64_t        acceptedRunID;
    Buffer          acceptedValue;

};

/*
===============================================================================================
*/

inline void PaxosAcceptorState::Init()
{
    OnNewPaxosRound();
}

inline void PaxosAcceptorState::OnNewPaxosRound()
{
    promisedProposalID = 0;
    accepted = false;
    acceptedProposalID = 0;
    acceptedRunID = 0;
    acceptedValue.Clear();
}

#endif
