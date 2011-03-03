#ifndef PAXOSPROPOSER_H
#define PAXOSPROPOSER_H

#include "System/Common.h"
#include "System/Events/Countdown.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosMessage.h"
#include "States/PaxosProposerState.h"

class ReplicatedLog; // forward

#define PAXOS_TIMEOUT   1*1000

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
    
    void                        OnMessage(PaxosMessage& msg);
    void                        OnPrepareTimeout();
    void                        OnProposeTimeout();
    
    void                        Propose(Buffer& value);
    void                        Restart();

    void                        Stop();
    bool                        IsActive(); 

private:
    void                        OnPrepareResponse(PaxosMessage& msg);
    void                        OnProposeResponse(PaxosMessage& msg);

    void                        BroadcastMessage(PaxosMessage& msg);
    void                        StopPreparing();
    void                        StopProposing();
    void                        StartPreparing();
    void                        StartProposing();
    void                        NewVote();

    QuorumContext*              context;
    QuorumVote*                 vote;
    PaxosProposerState          state;
    Countdown                   prepareTimeout;
    Countdown                   proposeTimeout;
    
    friend class ReplicatedLog;
};

#endif
