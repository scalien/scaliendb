#include "PaxosLease.h"
#include "System/Common.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"

PaxosLease::PaxosLease()
{
	startupTimeout.SetDelay(MAX_LEASE_TIME);
	startupTimeout.SetCallable(MFUNC(PaxosLease, OnStartupTimeout));
}

void PaxosLease::Init(QuorumContext* context_)
{
	context = context_;
	context->SetOnMessage(MFUNC(PaxosLease, OnMessage));
	
	EventLoop::Reset(&startupTimeout);
	
	proposer.Init(context_);
	acceptor.Init(context_);
	learner.Init(context_);
	learner.SetOnLearnLease(MFUNC(PaxosLease, OnLearnLease));
	learner.SetOnLeaseTimeout(MFUNC(PaxosLease, OnLeaseTimeout));
	
	acquireLease = false;
}

void PaxosLease::OnMessage()
{
	ReadBuffer readBuffer;
	PaxosLeaseMessage msg;

	readBuffer = context->GetTransport()->GetMessage();
	if (!msg.Read(readBuffer))
		ASSERT_FAIL();

	if ((msg.IsRequest()) && msg.proposalID > proposer.HighestProposalID())
			proposer.SetHighestProposalID(msg.proposalID);
	
	if (msg.IsResponse())
		proposer.OnMessage(msg);
	else if (msg.IsRequest())
		acceptor.OnMessage(msg);
	else if (msg.IsLearnChosen())
		learner.OnMessage(msg);
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

uint64_t PaxosLease::GetLeaseEpoch()
{
	return learner.GetLeaseEpoch();
}

void PaxosLease::OnStartupTimeout()
{
	Log_Trace();
}

void PaxosLease::OnLearnLease()
{
	Log_Trace();
	
	context->SetMaster(learner.GetLeaseOwner());
	if (!IsLeaseOwner())
		proposer.StopAcquireLease();
}

void PaxosLease::OnLeaseTimeout()
{
	Log_Trace();
	
	context->SetNoMaster();
	if (acquireLease)
		proposer.StartAcquireLease();
}
