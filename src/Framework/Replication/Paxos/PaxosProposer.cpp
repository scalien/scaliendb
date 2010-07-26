#include "PaxosProposer.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosProposer::Init(QuorumContext* context_)
{
	context = context_;
	vote = context->GetQuorum()->NewVote();
	
	prepareTimeout.SetCallable(MFUNC(PaxosProposer, OnPrepareTimeout));
	proposeTimeout.SetCallable(MFUNC(PaxosProposer, OnProposeTimeout));
	prepareTimeout.SetDelay(PAXOS_TIMEOUT);
	proposeTimeout.SetDelay(PAXOS_TIMEOUT);

	state.Init();
}

void PaxosProposer::OnMessage(const PaxosMessage& imsg)
{
	if (imsg.IsPrepareResponse())
		OnPrepareResponse(imsg);
	else if (imsg.IsProposeResponse())
		OnProposeResponse(imsg);
	else
		ASSERT_FAIL();
}

void PaxosProposer::OnPrepareTimeout()
{
	Log_Trace();
	
	assert(state.preparing);
	
	StartPreparing();
}

void PaxosProposer::OnProposeTimeout()
{
	Log_Trace();
	
	assert(state.proposing);

	StartPreparing();
}

void PaxosProposer::Propose(const Buffer& value)
{
	Log_Trace();
	
	if (IsActive())
		ASSERT_FAIL();

	state.proposedRunID = RMAN->GetRunID();
	state.proposedValue.Write(value);
	
	if (state.multi && state.numProposals == 0)
	{
		state.numProposals++;
		StartProposing();
	}
	else
		StartPreparing();	
}

void PaxosProposer::Stop()
{
	state.proposedRunID = 0;
	state.proposedValue.Clear();
	state.preparing = false;
	state.proposing = false;
	EventLoop::Remove(&prepareTimeout);
	EventLoop::Remove(&proposeTimeout);
}

bool PaxosProposer::IsActive()
{
	return (state.preparing || state.proposing);
}

void PaxosProposer::OnPrepareResponse(const PaxosMessage& imsg)
{
	Log_Trace("msg.nodeID = %u", imsg.nodeID);

	if (!state.preparing || imsg.proposalID != state.proposalID)
		return;
	
	if (imsg.type == PAXOS_PREPARE_REJECTED)
		vote->RegisterRejected(imsg.nodeID);
	else
		vote->RegisterAccepted(imsg.nodeID);
	
	if (imsg.type == PAXOS_PREPARE_REJECTED)
	{
		if (imsg.promisedProposalID > state.highestPromisedProposalID)
			state.highestPromisedProposalID = imsg.promisedProposalID;
	}
	else if (imsg.type == PAXOS_PREPARE_PREVIOUSLY_ACCEPTED &&
			 imsg.acceptedProposalID >= state.highestReceivedProposalID)
	{
		/* the >= could be replaced by > which would result in less copys
		 * however this would result in complications in multi paxos
		 * in the multi paxos steady state this branch is inactive
		 * it only runs after leader failure
		 * so it's ok
		 */
		state.highestReceivedProposalID = imsg.acceptedProposalID;
		state.proposedRunID = imsg.runID;
		state.proposedValue.Write(*imsg.value);
	}

	if (vote->IsRoundRejected())
		StartPreparing();
	else if (vote->IsRoundAccepted())
		StartProposing();	
	else if (vote->IsRoundComplete())
		StartPreparing();
}


void PaxosProposer::OnProposeResponse(const PaxosMessage& imsg)
{
	PaxosMessage omsg;
	
	Log_Trace("msg.nodeID = %u", imsg.nodeID);
	
	if (!state.proposing || imsg.proposalID != state.proposalID)
		return;
	
	if (imsg.type == PAXOS_PROPOSE_REJECTED)
		vote->RegisterRejected(imsg.nodeID);
	else
		vote->RegisterAccepted(imsg.nodeID);

	// see if we have enough positive replies to advance
	if (vote->IsRoundAccepted())
	{
		// a majority have accepted our proposal, we have consensus
		StopProposing();
		omsg.LearnProposal(context->GetPaxosID(), RMAN->GetNodeID(), state.proposalID);
		BroadcastMessage(omsg);
	}
	else if (vote->IsRoundComplete())
		StartPreparing();
}

void PaxosProposer::BroadcastMessage(const PaxosMessage& omsg)
{
	Log_Trace();
	
	vote->Reset();
	context->GetTransport()->BroadcastMessage(omsg);
}

void PaxosProposer::StopPreparing()
{
	Log_Trace();

	state.preparing = false;
	EventLoop::Remove(&prepareTimeout);
}

void PaxosProposer::StopProposing()
{
	Log_Trace();
	
	state.proposing = false;
	EventLoop::Remove(&proposeTimeout);
}

void PaxosProposer::StartPreparing()
{
	PaxosMessage omsg;
	
	Log_Trace();

	StopProposing();

	state.preparing = true;
	state.numProposals++;
	state.proposalID = RMAN->NextProposalID(MAX(state.proposalID, state.highestPromisedProposalID));
	state.highestReceivedProposalID = 0;
	
	omsg.PrepareRequest(context->GetPaxosID(), RMAN->GetNodeID(), state.proposalID);
	BroadcastMessage(omsg);
	
	EventLoop::Reset(&prepareTimeout);
}

void PaxosProposer::StartProposing()
{
	PaxosMessage omsg;
	
	Log_Trace();
	
	StopPreparing();

	state.proposing = true;
	
	omsg.ProposeRequest(context->GetPaxosID(), RMAN->GetNodeID(), state.proposalID,
	 state.proposedRunID, &state.proposedValue);
	BroadcastMessage(omsg);
	
	EventLoop::Reset(&proposeTimeout);
}
