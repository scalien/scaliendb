#include "PaxosLeaseProposer.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosLeaseProposer::Init(ReplicationContext* context_)
{
	context = context_;
	
	acquireLeaseTimeout.SetCallable(MFUNC(PaxosLeaseProposer, OnAcquireLeaseTimeout));
	acquireLeaseTimeout.SetDelay(ACQUIRELEASE_TIMEOUT);
	extendLeaseTimeout.SetCallable(MFUNC(PaxosLeaseProposer, OnExtendLeaseTimeout));
	
	highestProposalID = 0;
	state.Init();
}

void PaxosLeaseProposer::OnMessage(const PaxosLeaseMessage& imsg)
{
	if (imsg.IsPrepareResponse())
		OnPrepareResponse(imsg);
	else if (imsg.IsProposeResponse())
		OnProposeResponse(imsg);
	else
		ASSERT_FAIL();
}

void PaxosLeaseProposer::OnNewPaxosRound()
{
	// since PaxosLease msgs carry the paxosID, and nodes
	// won't reply if their paxosID is larger than the msg's
	// if the paxosID increases we must restart the
	// PaxosLease round, if it's active
	// only restart if we're masters
	
	//Log_Trace();
	//
	//if (acquireLeaseTimeout.IsActive() && RLOG->IsMaster())
	//	StartPreparing();
}

void PaxosLeaseProposer::StartAcquireLease()
{
	Log_Trace();
	
	EventLoop::Remove(&extendLeaseTimeout);
	
	if (!(state.preparing || state.proposing))
		StartPreparing();
}

void PaxosLeaseProposer::StopAcquireLease()
{
	Log_Trace();
	
	state.preparing = false;
	state.proposing = false;
	EventLoop::Remove(&extendLeaseTimeout);
	EventLoop::Remove(&acquireLeaseTimeout);
}

uint64_t PaxosLeaseProposer::GetHighestProposalID()
{
	return highestProposalID;
}

void PaxosLeaseProposer::SetHighestProposalID(uint64_t highestProposalID_)
{
	highestProposalID = highestProposalID_;
}

void PaxosLeaseProposer::OnAcquireLeaseTimeout()
{
	Log_Trace();
		
	StartPreparing();
}

void PaxosLeaseProposer::OnExtendLeaseTimeout()
{
	Log_Trace();
	
	assert(!(state.preparing || state.proposing));
	
	StartPreparing();
}

void PaxosLeaseProposer::OnPrepareResponse(const PaxosLeaseMessage& imsg)
{
	Log_Trace();

	if (!state.preparing || imsg.proposalID != state.proposalID)
		return;
	
	if (imsg.type == PAXOSLEASE_PREPARE_REJECTED)
		context->GetQuorum()->RegisterRejected(imsg.nodeID);
	else
		context->GetQuorum()->RegisterAccepted(imsg.nodeID);
	
	if (imsg.type == PAXOSLEASE_PREPARE_PREVIOUSLY_ACCEPTED && 
	 imsg.acceptedProposalID >= state.highestReceivedProposalID)
	{
		state.highestReceivedProposalID = imsg.acceptedProposalID;
		state.leaseOwner = imsg.leaseOwner;
	}

	if (context->GetQuorum()->IsRoundRejected())
		StartPreparing();
	else if (context->GetQuorum()->IsRoundAccepted())
		StartProposing();	
	else if (context->GetQuorum()->IsRoundComplete())
		StartPreparing();
}

void PaxosLeaseProposer::OnProposeResponse(const PaxosLeaseMessage& imsg)
{
	PaxosLeaseMessage omsg;
	
	Log_Trace();

	if (state.expireTime < Now())
	{
		Log_Trace("already expired, wait for timer");
		return;
	}
	
	if (!state.proposing || imsg.proposalID != state.proposalID)
		return;
	
	if (imsg.type == PAXOSLEASE_PROPOSE_REJECTED)
		context->GetQuorum()->RegisterRejected(imsg.nodeID);
	else
		context->GetQuorum()->RegisterAccepted(imsg.nodeID);
	
	// see if we have enough positive replies to advance
	if (context->GetQuorum()->IsRoundAccepted() && state.expireTime - Now() > 500 /*msec*/)
	{		
		// a majority have accepted our proposal, we have consensus
		EventLoop::Remove(&acquireLeaseTimeout);
		extendLeaseTimeout.Set(Now() + (state.expireTime - Now()) / 7);
		EventLoop::Reset(&extendLeaseTimeout);
	
		omsg.LearnChosen(RMAN->GetNodeID(), state.leaseOwner,
		 state.expireTime - Now(), state.expireTime);
		BroadcastMessage(omsg);

		state.proposing = false;
	}
	else if (context->GetQuorum()->IsRoundComplete())
		StartPreparing();
}

void PaxosLeaseProposer::BroadcastMessage(const PaxosLeaseMessage& omsg)
{
	Log_Trace();
	
	context->GetQuorum()->Reset();
	context->GetTransport()->BroadcastMessage(omsg);
}

void PaxosLeaseProposer::StartPreparing()
{
	PaxosLeaseMessage omsg;

	Log_Trace();

	EventLoop::Reset(&acquireLeaseTimeout);

	state.proposing = false;
	state.preparing = true;
	state.leaseOwner = RMAN->GetNodeID();
	state.highestReceivedProposalID = 0;
	state.proposalID = RMAN->NextProposalID(highestProposalID);
		
	if (state.proposalID > highestProposalID)
		highestProposalID = state.proposalID;
	
	omsg.PrepareRequest(RMAN->GetNodeID(), state.proposalID, context->GetPaxosID());
	BroadcastMessage(omsg);
}

void PaxosLeaseProposer::StartProposing()
{
	PaxosLeaseMessage omsg;
	
	Log_Trace();
	
	state.preparing = false;

	if (state.leaseOwner != RMAN->GetNodeID())
		return; // no point in getting someone else a lease, wait for OnAcquireLeaseTimeout

	state.proposing = true;
	state.duration = MAX_LEASE_TIME;
	state.expireTime = Now() + state.duration;
	
	omsg.ProposeRequest(RMAN->GetNodeID(), state.proposalID, state.leaseOwner, state.duration);
	BroadcastMessage(omsg);
}
