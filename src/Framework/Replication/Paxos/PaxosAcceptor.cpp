#include "PaxosAcceptor.h"
#include "PaxosProposer.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosAcceptor::Init(QuorumContext* context_)
{
	context = context_;

	ReadState();
}

void PaxosAcceptor::OnMessage(const PaxosMessage& imsg)
{
	if (imsg.type == PAXOS_PREPARE_REQUEST)
		OnPrepareRequest(imsg);
	else if (imsg.type == PAXOS_PROPOSE_REQUEST)
		OnProposeRequest(imsg);
	else
		ASSERT_FAIL();
}

void PaxosAcceptor::OnPrepareRequest(const PaxosMessage& imsg)
{
	PaxosMessage omsg;
	
	Log_Trace();
	
	if (context->GetDatabase()->IsActive())
		return;

	senderID = imsg.nodeID;
	
	Log_Trace("state.promisedProposalID: %" PRIu64 " msg.proposalID: %" PRIu64 "",
	 state.promisedProposalID, imsg.proposalID);
	
	if (imsg.proposalID < state.promisedProposalID)
	{
		omsg.PrepareRejected(imsg.paxosID, RMAN->GetNodeID(),
		 imsg.proposalID, state.promisedProposalID);
		context->GetTransport()->SendMessage(senderID, omsg);
		return;
	}

	state.promisedProposalID = imsg.proposalID;

	if (!state.accepted)
		omsg.PrepareCurrentlyOpen(imsg.paxosID, RMAN->GetNodeID(), imsg.proposalID);
	else
		omsg.PreparePreviouslyAccepted(imsg.paxosID, RMAN->GetNodeID(),
		 imsg.proposalID, state.acceptedProposalID,
		 state.acceptedRunID, state.acceptedEpochID, &state.acceptedValue);
	
	WriteState();
	//RLOG->StopPaxos(); TODO
}

void PaxosAcceptor::OnProposeRequest(const PaxosMessage& imsg)
{
	PaxosMessage omsg;
	
	Log_Trace();
	
	if (context->GetDatabase()->IsActive())
		return;
	
	senderID = imsg.nodeID;

	Log_Trace("state.promisedProposalID: %" PRIu64 " msg.proposalID: %" PRIu64 "",
	 state.promisedProposalID, imsg.proposalID);
	
	if (imsg.proposalID < state.promisedProposalID)
	{
		omsg.ProposeRejected(imsg.paxosID, RMAN->GetNodeID(), imsg.proposalID);
		context->GetTransport()->SendMessage(senderID, omsg);
		return;
	}

	state.accepted = true;
	state.acceptedProposalID = imsg.proposalID;
	state.acceptedRunID = imsg.runID;
	state.acceptedEpochID = imsg.epochID;
	state.acceptedValue.Write(*imsg.value);
	omsg.ProposeAccepted(imsg.paxosID, RMAN->GetNodeID(), imsg.proposalID);
	
	WriteState();
	//RLOG->StopPaxos(); TODO
}

void PaxosAcceptor::OnStateWritten()
{
	Log_Trace();
	
	PaxosMessage omsg;

	if (writtenPaxosID != context->GetPaxosID())
		return;

	if (!state.accepted)
		omsg.PrepareCurrentlyOpen(context->GetPaxosID(),
		 RMAN->GetNodeID(), state.promisedProposalID);
	else
		omsg.PreparePreviouslyAccepted(context->GetPaxosID(), RMAN->GetNodeID(),
		 state.promisedProposalID, state.acceptedProposalID,
		 state.acceptedRunID, state.acceptedEpochID, &state.acceptedValue);

	context->GetTransport()->SendMessage(senderID, omsg);
}

void PaxosAcceptor::ReadState()
{
	Log_Trace();
	
	QuorumDatabase* db;
	
	db = context->GetDatabase();

	context->SetPaxosID(db->GetPaxosID());
	state.accepted = db->GetAccepted();
	state.promisedProposalID = db->GetPromisedProposalID();
	if (state.accepted)
	{
		state.acceptedRunID = db->GetAcceptedRunID();
		state.acceptedEpochID = db->GetAcceptedEpochID();
		state.acceptedProposalID = db->GetAcceptedProposalID();
		db->GetAcceptedValue(state.acceptedValue);
	}
}

void PaxosAcceptor::WriteState()
{
	QuorumDatabase* db;
	
	db = context->GetDatabase();
	
	db->Begin();
	db->SetPaxosID(context->GetPaxosID());
	db->SetAccepted(state.accepted);
	db->SetPromisedProposalID(state.promisedProposalID);
	if (state.accepted)
	{
		db->SetAcceptedRunID(state.acceptedRunID);
		db->SetAcceptedEpochID(state.acceptedEpochID);
		db->SetAcceptedProposalID(state.acceptedProposalID);
		db->SetAcceptedValue(state.acceptedValue);
	}

	writtenPaxosID = context->GetPaxosID();

	db->Commit();

	// this is a different function because it may be necessary to async this
	// right now this makes writtenPaxosID (and senderID) unecessary
	OnStateWritten();
}
