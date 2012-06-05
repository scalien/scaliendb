#ifndef REPLICATEDLOG_H
#define REPLICATEDLOG_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Paxos/PaxosProposer.h"
#include "Framework/Replication/Paxos/PaxosAcceptor.h"

#define REQUEST_CHOSEN_TIMEOUT      (1000)
#define CANARY_TIMEOUT              (60*1000) // 1 minute
#define PAXOS_CATCHUP_GRANULARITY   (100*KiB)

/*
===============================================================================================

 ReplicatedLog

===============================================================================================
*/

class ReplicatedLog
{
public:
    ReplicatedLog();

    void                    Init(QuorumContext* context);
    void                    Shutdown();

    uint64_t                GetLastLearnChosenTime();
    void                    SetAlwaysUseDatabaseCatchup(bool alwaysUseDatabaseCatchup);

    void                    Stop();
    void                    Continue();

    bool                    IsMultiPaxosEnabled();
    bool                    IsAppending();
    bool                    IsWaitingOnAppend();

    void                    TryAppendDummy();
    void                    TryAppendNextValue();
    void                    TryCatchup();
    void                    Restart();
    
    void                    SetPaxosID(uint64_t paxosID);
    uint64_t                GetPaxosID();
    void                    NewPaxosRound();
    
    void                    RegisterPaxosID(uint64_t paxosID, uint64_t nodeID);
    
    void                    OnMessage(PaxosMessage& msg);
    void                    OnCatchupStarted();
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
    void                    OnCanaryTimeout();

    void                    ProcessLearnChosen(uint64_t nodeID, uint64_t runID);

    void                    OnRequest(PaxosMessage& msg);
    void                    RequestChosen(uint64_t nodeID);

    QuorumContext*          context;

    uint64_t                paxosID;
    PaxosProposer           proposer;
    PaxosAcceptor           acceptor;
    
    bool                    waitingOnAppend;
    bool                    appendDummyNext;
    uint64_t                lastRequestChosenTime;
    uint64_t                lastLearnChosenTime;
    Countdown               canaryTimer;
};
#endif
