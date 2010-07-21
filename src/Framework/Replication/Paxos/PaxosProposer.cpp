#include "PaxosProposer.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosProposer::Init(ReplicationContext* context_)
{
	context = context_;
	
	prepareTimeout.SetCallable(MFUNC(PaxosProposer, OnPrepareTimeout));
	proposeTimeout.SetCallable(MFUNC(PaxosProposer, OnProposeTimeout));
	prepareTimeout.SetDelay(PAXOS_TIMEOUT);
	proposeTimeout.SetDelay(PAXOS_TIMEOUT);

	paxosID = 0;
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

bool PaxosProposer::Propose(const Buffer& value)
{
	Log_Trace();
	
	if (IsActive())
		ASSERT_FAIL();

	state.value.Write(value);
	
	if (state.leader && state.numProposals == 0)
	{
		state.numProposals++;
		StartProposing();
	}
	else
		StartPreparing();
	
	return true;
}

void PaxosProposer::Stop()
{
	state.value.Init();
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
		context->GetQuorum()->RegisterRejected(imsg.nodeID);
	else
		context->GetQuorum()->RegisterAccepted(imsg.nodeID);
	
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
		state.value.Write(*imsg.value);
	}

	if (context->GetQuorum()->IsRoundRejected())
		StartPreparing();
	else if (context->GetQuorum()->IsRoundAccepted())
		StartProposing();	
	else if (context->GetQuorum()->IsRoundComplete())
		StartPreparing();
}


void PaxosProposer::OnProposeResponse(const PaxosMessage& imsg)
{
	PaxosMessage omsg;
	
	Log_Trace("msg.nodeID = %u", imsg.nodeID);
	
	if (!state.proposing || imsg.proposalID != state.proposalID)
		return;
	
	if (imsg.type == PAXOS_PROPOSE_REJECTED)
		context->GetQuorum()->RegisterRejected(imsg.nodeID);
	else
		context->GetQuorum()->RegisterAccepted(imsg.nodeID);

	// see if we have enough positive replies to advance
	if (context->GetQuorum()->IsRoundAccepted())
	{
		// a majority have accepted our proposal, we have consensus
		StopProposing();
		omsg.LearnProposal(paxosID, RMAN->GetNodeID(), state.proposalID);
		BroadcastMessage(omsg);
	}
	else if (context->GetQuorum()->IsRoundComplete())
		StartPreparing();
}

void PaxosProposer::BroadcastMessage(const PaxosMessage& omsg)
{
	Log_Trace();
	
	context->GetQuorum()->Reset();
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
	
	omsg.PrepareRequest(paxosID, RMAN->GetNodeID(), state.proposalID);
	BroadcastMessage(omsg);
	
	EventLoop::Reset(&prepareTimeout);
}

void PaxosProposer::StartProposing()
{
	PaxosMessage omsg;
	
	Log_Trace();
	
	StopPreparing();

	state.proposing = true;
	
	omsg.ProposeRequest(paxosID, RMAN->GetNodeID(), state.proposalID, &state.value);
	BroadcastMessage(omsg);
	
	EventLoop::Reset(&proposeTimeout);
}
