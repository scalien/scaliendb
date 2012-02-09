#ifndef PAXOSLEASELEARNER_H
#define PAXOSLEASELEARNER_H

#include "System/Common.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
#include "Framework/Replication/PaxosLease/States/PaxosLeaseLearnerState.h"
#include "PaxosLeaseMessage.h"

/*
===============================================================================================

 PaxosLeaseLearner

===============================================================================================
*/

class PaxosLeaseLearner
{
    typedef PaxosLeaseLearnerState LearnerState;

public:
    void            Init(QuorumContext* context);
    bool            IsLeaseOwner();
    bool            IsLeaseKnown();
    uint64_t        GetLeaseOwner();
    void            SetOnLearnLease(Callable onLearnLeaseCallback);
    void            SetOnLeaseTimeout(Callable onLeaseTimeoutCallback);
    void            SetOnAcquireLease(Callable onAcquireLeaseCallback);

    void            OnMessage(PaxosLeaseMessage& msg);

private:
    void            OnLeaseTimeout();
    void            OnLearnChosen(PaxosLeaseMessage& msg);
    void            CheckLease();

    QuorumContext*  context;
    LearnerState    state;  
    Timer           leaseTimeout;
    Callable        onLearnLeaseCallback;
    Callable        onLeaseTimeoutCallback;
};

#endif
