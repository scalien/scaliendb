#include "PaxosAcceptor.h"
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include "System/Log.h"
#include "System/Events/EventLoop.h"
#include "Framework/Replication/ReplicationManager.h"

void PaxosAcceptor::Init(ReplicationContext* context_)
{
	context = context_;

	paxosID = 0;
	state.Init();

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
		 imsg.proposalID, state.acceptedProposalID, &state.acceptedValue);
	
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
	state.acceptedValue.Write(*imsg.value);
	omsg.ProposeAccepted(imsg.paxosID, RMAN->GetNodeID(), imsg.proposalID);
	
	WriteState();
	//RLOG->StopPaxos(); TODO
}

void PaxosAcceptor::OnStateWritten()
{
	Log_Trace();
	
	PaxosMessage omsg;

	if (writtenPaxosID != paxosID)
		return;

	if (!state.accepted)
		omsg.PrepareCurrentlyOpen(paxosID, RMAN->GetNodeID(), state.promisedProposalID);
	else
		omsg.PreparePreviouslyAccepted(paxosID, RMAN->GetNodeID(),
		 state.promisedProposalID, state.acceptedProposalID, &state.acceptedValue);

	context->GetTransport()->SendMessage(senderID, omsg);
}

void PaxosAcceptor::ReadState()
{
	Log_Trace();
	
	QuorumDatabase* db;
	
	db = context->GetDatabase();

	paxosID = db->GetPaxosID();
	state.accepted = db->GetAccepted();
	state.promisedProposalID = db->GetPromisedProposalID();
	if (state.accepted)
	{
		state.acceptedProposalID = db->GetAcceptedProposalID();
		state.acceptedValue.Write(*db->GetAcceptedValue()); // TODO?
	}
}

void PaxosAcceptor::WriteState()
{
	QuorumDatabase* db;
	
	db = context->GetDatabase();
	
	db->Begin();
	db->SetPaxosID(paxosID);
	db->SetAccepted(state.accepted);
	db->SetPromisedProposalID(state.promisedProposalID);
	if (state.accepted)
	{
		db->SetAcceptedProposalID(state.acceptedProposalID);
		db->SetAcceptedValue(&state.acceptedValue);
	}

	writtenPaxosID = paxosID;

	db->Commit(MFUNC(PaxosAcceptor, OnStateWritten));
}
