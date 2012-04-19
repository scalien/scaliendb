#include "PaxosLeaseLearner.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationConfig.h"

#define UNDEFINED_NODEID    ((uint64_t)(-1))

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
    
    if (state.learned)
        return state.leaseOwner;
    else
        return UNDEFINED_NODEID;
}

void PaxosLeaseLearner::SetOnLearnLease(Callable onLearnLeaseCallback_)
{
    onLearnLeaseCallback = onLearnLeaseCallback_;
}

void PaxosLeaseLearner::SetOnLeaseTimeout(Callable onLeaseTimeoutCallback_)
{
    onLeaseTimeoutCallback = onLeaseTimeoutCallback_;
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
    Log_Trace();

    EventLoop::Remove(&leaseTimeout); // because OnLeaseTimeout() is also called explicitly

    state.OnLeaseTimeout();
    
    Call(onLeaseTimeoutCallback);
}

void PaxosLeaseLearner::OnLearnChosen(PaxosLeaseMessage& imsg)
{
    uint64_t expireTime;

    Log_Trace();

    CheckLease();
    
    if (state.learned && state.leaseOwner != imsg.leaseOwner)
    {
        // should not happen
        // majority of nodes' clocks are ahead of the old lease owner
        // most likely the old lease owner had his clock reset backwards
        Log_Message("New lease owner does not match state lease owner! System clock reset backwards on one of the nodes?");
        Log_Debug("state.leaseOwner = %U, imsg.leaseOwner = %U", state.leaseOwner, imsg.leaseOwner);
        OnLeaseTimeout();
    }

    if (imsg.leaseOwner == MY_NODEID)
        expireTime = imsg.localExpireTime; // I'm the master
    else
        expireTime = Now() + imsg.duration - 500 /* msec */;
        // conservative estimate
    
    if (expireTime < Now())
        return;
    
    state.learned = true;
    state.leaseOwner = imsg.leaseOwner;
    state.expireTime = expireTime;
    Log_Trace("+++ Node %U has the lease for %U msec +++",
     state.leaseOwner, state.expireTime - Now());

    leaseTimeout.SetExpireTime(state.expireTime);
    EventLoop::Reset(&leaseTimeout);
    
    Call(onLearnLeaseCallback);
}

void PaxosLeaseLearner::CheckLease()
{
    uint64_t now;
    
    now = EventLoop::Now();
    
    if (state.learned && state.expireTime < now)
        OnLeaseTimeout();
}
