#ifndef PAXOSPROPOSER_H
#define PAXOSPROPOSER_H

#include "System/Common.h"
#include "System/Events/Countdown.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosMessage.h"
#include "States/PaxosProposerState.h"

class ReplicatedLog; // forward

#define PAXOS_ROUND_TIMEOUT     (5*1000)
#define PAXOS_RESTART_TIMEOUT   (100)

/*
===============================================================================================

 PaxosProposer

===============================================================================================
*/

class PaxosProposer
{
    typedef PaxosProposerState State;

public:
    ~PaxosProposer();
    
    void            Init(QuorumContext* context);
    void            RemoveTimers();
    void            SetUseTimeouts(bool useTimeouts);
    void            OnPrepareResponse(PaxosMessage& msg);
    void            OnProposeResponse(PaxosMessage& msg);
    void            Propose(Buffer& value);
    void            Restart();
    void            Stop();
    bool            IsActive();
    bool            IsLearnSent();
    uint64_t        GetMemoryUsage();

    State           state;

private:
    void            OnPrepareTimeout();
    void            OnProposeTimeout();
    void            OnRestartTimeout();
    void            BroadcastMessage(PaxosMessage& msg);
    void            StopPreparing();
    void            StopProposing();
    void            StartPreparing();
    void            StartProposing();
    void            NewVote();

    bool            useTimeouts;
    QuorumContext*  context;
    QuorumVote*     vote;
    Countdown       prepareTimeout;
    Countdown       proposeTimeout;
    Countdown       restartTimeout;
};

#endif
