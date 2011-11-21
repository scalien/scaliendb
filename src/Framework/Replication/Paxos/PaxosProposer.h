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
public:
    ~PaxosProposer();
    
    void                        Init(QuorumContext* context);
    void                        Shutdown();
    
    void                        SetUseTimeouts(bool useTimeouts);
    
    void                        OnMessage(PaxosMessage& msg);
    void                        OnPrepareTimeout();
    void                        OnProposeTimeout();
    void                        OnRestartTimeout();
    
    void                        Propose(Buffer& value);
    void                        Restart();

    void                        Stop();
    bool                        IsActive(); 

    uint64_t                    GetMemoryUsage();

private:
    void                        OnPrepareResponse(PaxosMessage& msg);
    void                        OnProposeResponse(PaxosMessage& msg);

    void                        BroadcastMessage(PaxosMessage& msg);
    void                        StopPreparing();
    void                        StopProposing();
    void                        StartPreparing();
    void                        StartProposing();
    void                        NewVote();

    bool                        useTimeouts;
    QuorumContext*              context;
    QuorumVote*                 vote;
    PaxosProposerState          state;
    Countdown                   prepareTimeout;
    Countdown                   proposeTimeout;
    Countdown                   restartTimeout;
    
    friend class ReplicatedLog;
};

#endif
