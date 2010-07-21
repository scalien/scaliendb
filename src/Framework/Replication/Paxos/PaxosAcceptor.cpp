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

	if (!ReadState())
		Log_Message("Database is empty");
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
	Log_Trace();
	
	if (mdbop.IsActive())
		return;

	msg = msg_;
	
	senderID = msg.nodeID;
	
	Log_Trace("state.promisedProposalID: %" PRIu64 " "
			   "msg.proposalID: %" PRIu64 "",
			   state.promisedProposalID, msg.proposalID);
	
	if (msg.proposalID < state.promisedProposalID)
	{
		msg.PrepareRejected(msg.paxosID,
			RCONF->GetNodeID(),
			msg.proposalID,
			state.promisedProposalID);
		
		context->GetTransport()->SendMessage(senderID, msg);
		return;
	}

	state.promisedProposalID = msg.proposalID;

	if (!state.accepted)
		msg.PrepareCurrentlyOpen(msg.paxosID,
			RCONF->GetNodeID(),
			msg.proposalID);
	else
		msg.PreparePreviouslyAccepted(msg.paxosID,
			RCONF->GetNodeID(),
			msg.proposalID,
			state.acceptedProposalID,
			state.acceptedValue);
	
	WriteState();
	RLOG->StopPaxos();
}

void PaxosAcceptor::OnProposeRequest(const PaxosMessage& imsg)
{
	Log_Trace();
	
	if (mdbop.IsActive())
		return;
	
	msg = msg_;
	
	senderID = msg.nodeID;

	Log_Trace("state.promisedProposalID: %" PRIu64 " "
				"msg.proposalID: %" PRIu64 "",
				state.promisedProposalID, msg.proposalID);
	
	if (msg.proposalID < state.promisedProposalID)
	{
		msg.ProposeRejected(msg.paxosID,
			RCONF->GetNodeID(),
			msg.proposalID);
		
		context->GetTransport()->SendMessage(senderID, msg);
		return;
	}

	state.accepted = true;
	state.acceptedProposalID = msg.proposalID;
	if (!state.acceptedValue.Set(msg.value))
		ASSERT_FAIL();
	msg.ProposeAccepted(msg.paxosID,
		RCONF->GetNodeID(),
		msg.proposalID);
	
	WriteState();
	RLOG->StopPaxos();
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

bool PaxosAcceptor::ReadState()
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
		state.acceptedValue.Write(db->GetAcceptedValue()); // TODO?
	}
}

bool PaxosAcceptor::WriteState()
{
	QuorumDatabase* db;
	
	db = context->GetDatabase();
	
	db->SetPaxosID(paxosID);
	db->SetAccepted(state.accepted);
	db->SetPromisedProposalID(state.promisedProposalID);
	if (state.accepted)
	{
		db->SetAcceptedProposalID(state.acceptedProposalID);
		db->SetAcceptedValue(state.acceptedValue);
	}

	writtenPaxosID = paxosID;

	db->Write(MFUNC(PaxosAcceptor, OnStateWritten));
}
