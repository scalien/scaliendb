#include "PaxosLease.h"
#include "System/Common.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"

void PaxosLease::Init(QuorumContext* context_)
{
	context = context_;
	
	proposer.Init(context_);
	acceptor.Init(context_);
	learner.Init(context_);
	learner.SetOnLearnLease(MFUNC(PaxosLease, OnLearnLease));
	learner.SetOnLeaseTimeout(MFUNC(PaxosLease, OnLeaseTimeout));
	
	acquireLease = false;

	startupTimeout.SetDelay(PAXOSLEASE_MAX_LEASE_TIME);
	startupTimeout.SetCallable(MFUNC(PaxosLease, OnStartupTimeout));
	EventLoop::Add(&startupTimeout);
}

void PaxosLease::OnMessage(const PaxosLeaseMessage& imsg)
{
	if (startupTimeout.IsActive())
		return;

	if ((imsg.IsRequest()) && imsg.proposalID > proposer.GetHighestProposalID())
			proposer.SetHighestProposalID(imsg.proposalID);
	
	if (imsg.IsResponse())
		proposer.OnMessage(imsg);
	else if (imsg.IsRequest())
		acceptor.OnMessage(imsg);
	else if (imsg.IsLearnChosen())
		learner.OnMessage(imsg);
	else
		ASSERT_FAIL();
}

void PaxosLease::AcquireLease()
{
	acquireLease = true;
	proposer.StartAcquireLease();
}

bool PaxosLease::IsLeaseOwner()
{
	return learner.IsLeaseOwner();
}

bool PaxosLease::IsLeaseKnown()
{
	return learner.IsLeaseKnown();
}

unsigned PaxosLease::GetLeaseOwner()
{
	return learner.GetLeaseOwner();
}

void PaxosLease::OnStartupTimeout()
{
	Log_Trace();
	
	AcquireLease();
}

void PaxosLease::OnLearnLease()
{
	Log_Trace();
	
	context->OnLearnLease();
	
	if (!IsLeaseOwner())
		proposer.StopAcquireLease();
}

void PaxosLease::OnLeaseTimeout()
{
	Log_Trace();
	
	context->OnLeaseTimeout();
	
	if (acquireLease)
		proposer.StartAcquireLease();
}
