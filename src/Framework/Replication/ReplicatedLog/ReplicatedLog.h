#ifndef REPLICATEDLOG_H
#define REPLICATEDLOG_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Paxos/PaxosProposer.h"
#include "Framework/Replication/Paxos/PaxosAcceptor.h"

#define REQUEST_CHOSEN_TIMEOUT  1000

/*
===============================================================================================

 ReplicatedLog

===============================================================================================
*/

class ReplicatedLog
{
public:
    void                    Init(QuorumContext* context);
    void                    Shutdown();

    void                    SetUseProposeTimeouts(bool useProposeTimeouts);
    void                    SetCommitChaining(bool commitChaining);
    bool                    GetCommitChaining();
    void                    SetAsyncCommit(bool asyncCommit);
    bool                    GetAsyncCommit();

    void                    Stop();
    void                    Continue();

    bool                    IsMultiPaxosEnabled();
    bool                    IsAppending();

    void                    TryAppendDummy();
    void                    TryAppendNextValue();
    void                    TryCatchup();
    void                    Restart();
    
    void                    SetPaxosID(uint64_t paxosID);
    uint64_t                GetPaxosID();
    void                    NewPaxosRound();
    void                    ResetPaxosState();
    
    void                    RegisterPaxosID(uint64_t paxosID, uint64_t nodeID);
    
    void                    OnMessage(PaxosMessage& msg);
    void                    OnCatchupComplete(uint64_t paxosID);
    void                    OnLearnLease();
    void                    OnLeaseTimeout();

    void                    OnAppendComplete();
    void                    WriteState();

    uint64_t                GetMemoryUsage();

private:
    void                    Append(Buffer& value);

    bool                    OnPrepareRequest(PaxosMessage& msg);
    bool                    OnPrepareResponse(PaxosMessage& msg);
    bool                    OnProposeRequest(PaxosMessage& msg);
    bool                    OnProposeResponse(PaxosMessage& msg);
    bool                    OnLearnChosen(PaxosMessage& msg);
    bool                    OnRequestChosen(PaxosMessage& msg);
    bool                    OnStartCatchup(PaxosMessage& msg);

    void                    ProcessLearnChosen(uint64_t nodeID, uint64_t runID);

    void                    OnRequest(PaxosMessage& msg);
    void                    RequestChosen(uint64_t nodeID);

    QuorumContext*          context;

    uint64_t                paxosID;
    PaxosProposer           proposer;
    PaxosAcceptor           acceptor;
    
    bool                    waitingOnAppend;
    bool                    useProposeTimeouts;
    bool                    commitChaining;
    uint64_t                lastRequestChosenTime;
};
#endif
