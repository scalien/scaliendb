#include "PaxosLeaseAcceptor.h"
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include "System/Log.h"
#include "System/Events/EventLoop.h"
#include "System/Config.h"
#include "System/Time.h"
#include "Framework/Replication/ReplicationManager.h"
#include "PaxosLease.h"

void PaxosLeaseAcceptor::Init(ReplicationContext* context_)
{
	context = context_;
	leaseTimeout.SetCallable(MFUNC(PaxosLeaseAcceptor, OnLeaseTimeout));
	state.Init();
}

void PaxosLeaseAcceptor::OnMessage(PaxosLeaseMessage* msg)
{
	if (msg->type == PAXOSLEASE_PREPARE_REQUEST)
		OnPrepareRequest(msg);
	else if (msg->type == PAXOSLEASE_PROPOSE_REQUEST)
		OnProposeRequest(msg);
	else
		ASSERT_FAIL();
}

void PaxosLeaseAcceptor::OnPrepareRequest(PaxosLeaseMessage* msg)
{
	Log_Trace("msg.paxosID: %" PRIu64 ", my.paxosID: %" PRIu64 "",
	 msg->paxosID, context->GetPaxosID());

	if (msg->paxosID < context->GetPaxosID() && (int) msg->nodeID != context->GetMaster())
		return; // only up-to-date nodes can become masters

//	RLOG->OnPaxosLeaseMessage(msg.paxosID, msg.nodeID); //TODO
	
	unsigned senderID = msg->nodeID;
	
	if (state.accepted && state.acceptedExpireTime < Now())
	{
		EventLoop::Remove(&leaseTimeout);
		OnLeaseTimeout();
	}
	
	if (msg->proposalID < state.promisedProposalID)
		msg->PrepareRejected(RMAN->GetNodeID(), msg->proposalID);
	else
	{
		state.promisedProposalID = msg->proposalID;

		if (!state.accepted)
			msg->PrepareCurrentlyOpen(RMAN->GetNodeID(), msg->proposalID);
		else
			msg->PreparePreviouslyAccepted(RMAN->GetNodeID(), msg->proposalID,
			 state.acceptedProposalID, state.acceptedLeaseOwner, state.acceptedDuration);
	}
	
	context->GetTransport()->SendMessage(senderID, msg);
}

void PaxosLeaseAcceptor::OnProposeRequest(PaxosLeaseMessage* msg)
{
	Log_Trace();
	
	unsigned senderID = msg->nodeID;
	
	if (state.accepted && state.acceptedExpireTime < Now())
	{
		EventLoop::Remove(&leaseTimeout);
		OnLeaseTimeout();
	}
	
	if (msg->proposalID < state.promisedProposalID)
		msg->ProposeRejected(RMAN->GetNodeID(), msg->proposalID);
	else
	{
		state.accepted = true;
		state.acceptedProposalID = msg->proposalID;
		state.acceptedLeaseOwner = msg->leaseOwner;
		state.acceptedDuration = msg->duration;
		state.acceptedExpireTime = Now() + state.acceptedDuration;
		
		leaseTimeout.Set(state.acceptedExpireTime);
		EventLoop::Reset(&leaseTimeout);
		
		msg->ProposeAccepted(RMAN->GetNodeID(), msg->proposalID);
	}
	
	context->GetTransport()->SendMessage(senderID, msg);
}

void PaxosLeaseAcceptor::OnLeaseTimeout()
{
	Log_Trace();
	
	state.OnLeaseTimeout();
}
