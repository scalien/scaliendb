#ifndef PAXOSLEASE_H
#define PAXOSLEASE_H

#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosLeaseMessage.h"
#include "PaxosLeaseProposer.h"
#include "PaxosLeaseAcceptor.h"
#include "PaxosLeaseLearner.h"

#define PAXOSLEASE_ACQUIRELEASE_TIMEOUT     2000    // msec
#define PAXOSLEASE_MAX_LEASE_TIME           3000    // msec

/*
===============================================================================================

 PaxosLease

===============================================================================================
*/

class PaxosLease
{
    typedef PaxosLeaseProposer Proposer;
    typedef PaxosLeaseAcceptor Acceptor;
    typedef PaxosLeaseLearner  Learner;

public:
    void            Init(QuorumContext* context);
    void            Stop();
    void            Continue();
    void            OnMessage(PaxosLeaseMessage& msg);
    void            AcquireLease();
    bool            IsLeaseOwner();
    bool            IsLeaseKnown();
    uint64_t        GetLeaseOwner();
    
private:
    void            OnStartupTimeout();
    void            OnLearnLease();
    void            OnLeaseTimeout();
    void            OnAcquireLease();

    QuorumContext*  context;
    Proposer        proposer;
    Acceptor        acceptor;
    Learner         learner;
    Countdown       startupTimeout;
    bool            active;
};

#endif
