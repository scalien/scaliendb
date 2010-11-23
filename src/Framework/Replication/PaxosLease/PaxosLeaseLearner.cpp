#include "PaxosLeaseLearner.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationConfig.h"

void PaxosLeaseLearner::Init(QuorumContext* context_)
{
    context = context_;
    leaseTimeout.SetCallable(MFUNC(PaxosLeaseLearner, OnLeaseTimeout));
    state.Init();
}

bool PaxosLeaseLearner::IsLeaseOwner()
{
    CheckLease();
    
    if (state.learned && state.leaseOwner == MY_NODEID)
        return true;
    
    return false;       
}

bool PaxosLeaseLearner::IsLeaseKnown()
{
    CheckLease();
    
    if (state.learned)
        return true;
    else
        return false;   
}

uint64_t PaxosLeaseLearner::GetLeaseOwner()
{
    CheckLease();
    
    return state.leaseOwner;
}

void PaxosLeaseLearner::SetOnLearnLease(Callable onLearnLeaseCallback_)
{
    onLearnLeaseCallback = onLearnLeaseCallback_;
}

void PaxosLeaseLearner::SetOnLeaseTimeout(Callable onLeaseTimeoutCallback_)
{
    onLeaseTimeoutCallback = onLeaseTimeoutCallback_;
}

void PaxosLeaseLearner::SetOnAcquireLease(Callable onAcquireLeaseCallback_)
{
    onAcquireLeaseCallback = onAcquireLeaseCallback_;
}

void PaxosLeaseLearner::OnMessage(PaxosLeaseMessage& imsg)
{
    if (imsg.type == PAXOSLEASE_LEARN_CHOSEN)
        OnLearnChosen(imsg);
    else
        ASSERT_FAIL();
}

void PaxosLeaseLearner::OnLeaseTimeout()
{
//  TODO
//  if (state.learned && RLOG->IsMasterLeaseActive())
//      Log_Message("Node %d is no longer the master", state.leaseOwner);

    Log_Trace();

    EventLoop::Remove(&leaseTimeout);

    state.OnLeaseTimeout();
    
    Call(onLeaseTimeoutCallback);
}

void PaxosLeaseLearner::OnLearnChosen(PaxosLeaseMessage& imsg)
{
    bool    leaseAcquired;
    
    Log_Trace();
    
    if (state.learned && state.expireTime < Now())
        OnLeaseTimeout();

    uint64_t expireTime;
    if (imsg.leaseOwner == MY_NODEID)
        expireTime = imsg.localExpireTime; // I'm the master
    else
        expireTime = Now() + imsg.duration - 500 /* msec */;
        // conservative estimate
    
    if (expireTime < Now())
        return;

    leaseAcquired = false;
    if (!state.learned)
        leaseAcquired = true;
    
    state.learned = true;
    state.leaseOwner = imsg.leaseOwner;
    state.expireTime = expireTime;
    Log_Trace("+++ Node %U has the lease for %U msec +++",
     state.leaseOwner, state.expireTime - Now());

    leaseTimeout.SetExpireTime(state.expireTime);
    EventLoop::Reset(&leaseTimeout);
    
    Call(onLearnLeaseCallback);
    if (leaseAcquired)
    {
        // TODO: HACK
        Log_Message("Node %U became the master", imsg.leaseOwner);
        Call(onAcquireLeaseCallback);
    }
}

void PaxosLeaseLearner::CheckLease()
{
    uint64_t now;
    
    now = EventLoop::Now();
    
    if (state.learned && state.expireTime < now)
        OnLeaseTimeout();
}
