#ifndef PAXOSPROPOSERSTATE_H
#define PAXOSPROPOSERSTATE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================

 PaxosProposerState

===============================================================================
*/

struct PaxosProposerState
{
    void            Init();
    void            OnNewPaxosRound();
    bool            Active();

    bool            preparing;
    bool            proposing;
    uint64_t        proposalID;
    uint64_t        highestReceivedProposalID;
    uint64_t        highestPromisedProposalID;

    uint64_t        proposedRunID;
    Buffer          proposedValue;

    bool            multi;        // multi paxos
    unsigned        numProposals; // number of proposal runs in this Paxos round
};

/*
===============================================================================
*/

inline void PaxosProposerState::Init()
{
    OnNewPaxosRound();
    multi = false;
}

inline void PaxosProposerState::OnNewPaxosRound()
{
    preparing = false;
    proposing = false;
    proposalID = 0;
    highestReceivedProposalID = 0;
    highestPromisedProposalID = 0;
    proposedRunID = 0;
    proposedValue.Clear();
    numProposals = 0;
}

inline bool PaxosProposerState::Active()
{
    return (preparing || proposing);
}

#endif
