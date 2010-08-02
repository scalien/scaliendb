#include "PaxosLeaseLearner.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosLeaseLearner::Init(QuorumContext* context_)
{
	context = context_;
	leaseTimeout.SetCallable(MFUNC(PaxosLeaseLearner, OnLeaseTimeout));
	state.Init();
}

bool PaxosLeaseLearner::IsLeaseOwner()
{
	CheckLease();
	
	if (state.learned && state.leaseOwner == RMAN->GetNodeID())
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

int PaxosLeaseLearner::GetLeaseOwner()
{
	CheckLease();
	
	if (state.learned)
		return state.leaseOwner;
	else
		return -1;
}

uint64_t PaxosLeaseLearner::GetLeaseEpoch()
{
	CheckLease();
	
	return state.leaseEpoch;
}

void PaxosLeaseLearner::SetOnLearnLease(const Callable& onLearnLeaseCallback_)
{
	onLearnLeaseCallback = onLearnLeaseCallback_;
}

void PaxosLeaseLearner::SetOnLeaseTimeout(const Callable& onLeaseTimeoutCallback_)
{
	onLeaseTimeoutCallback = onLeaseTimeoutCallback_;
}

void PaxosLeaseLearner::OnMessage(const PaxosLeaseMessage& imsg)
{
	if (imsg.type == PAXOSLEASE_LEARN_CHOSEN)
		OnLearnChosen(imsg);
	else
		ASSERT_FAIL();
}

void PaxosLeaseLearner::OnLeaseTimeout()
{
//	TODO
//	if (state.learned && RLOG->IsMasterLeaseActive())
//		Log_Message("Node %d is no longer the master", state.leaseOwner);

	EventLoop::Remove(&leaseTimeout);

	state.OnLeaseTimeout();
	
	Call(onLeaseTimeoutCallback);
}

void PaxosLeaseLearner::OnLearnChosen(const PaxosLeaseMessage& imsg)
{
	Log_Trace();
	
	if (state.learned && state.expireTime < Now())
		OnLeaseTimeout();

	uint64_t expireTime;
	if (imsg.leaseOwner == RMAN->GetNodeID())
		expireTime = imsg.localExpireTime; // I'm the master
	else
		expireTime = Now() + imsg.duration - 500 /* msec */;
		// conservative estimate
	
	if (expireTime < Now())
		return;
	
//	TODO
//	if (!state.learned && RLOG->IsMasterLeaseActive())
//		Log_Message("Node %d is the master", msg.leaseOwner);
	
	state.learned = true;
	state.leaseOwner = imsg.leaseOwner;
	state.expireTime = expireTime;
	Log_Trace("+++ Node %d has the lease for %" PRIu64 " msec +++",
		state.leaseOwner, state.expireTime - Now());

	leaseTimeout.Set(state.expireTime);
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
