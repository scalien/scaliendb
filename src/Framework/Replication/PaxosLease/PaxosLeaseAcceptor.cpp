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

void PaxosLeaseAcceptor::OnMessage(const PaxosLeaseMessage& imsg)
{
	if (imsg.type == PAXOSLEASE_PREPARE_REQUEST)
		OnPrepareRequest(imsg);
	else if (imsg.type == PAXOSLEASE_PROPOSE_REQUEST)
		OnProposeRequest(imsg);
	else
		ASSERT_FAIL();
}

void PaxosLeaseAcceptor::OnPrepareRequest(const PaxosLeaseMessage& imsg)
{
	PaxosLeaseMessage omsg;
	
	Log_Trace("msg.paxosID: %" PRIu64 ", my.paxosID: %" PRIu64 "",
	 imsg.paxosID, context->GetPaxosID());

	if (imsg.paxosID < context->GetPaxosID() && (int) imsg.nodeID != context->GetMaster())
		return; // only up-to-date nodes can become masters

//	RLOG->OnPaxosLeaseMessage(msg.paxosID, msg.nodeID); //TODO
	
	if (state.accepted && state.acceptedExpireTime < Now())
	{
		EventLoop::Remove(&leaseTimeout);
		OnLeaseTimeout();
	}
	
	if (imsg.proposalID < state.promisedProposalID)
		omsg.PrepareRejected(RMAN->GetNodeID(), imsg.proposalID);
	else
	{
		state.promisedProposalID = imsg.proposalID;

		if (!state.accepted)
			omsg.PrepareCurrentlyOpen(RMAN->GetNodeID(), imsg.proposalID);
		else
			omsg.PreparePreviouslyAccepted(RMAN->GetNodeID(), imsg.proposalID,
			 state.acceptedProposalID, state.acceptedLeaseOwner, state.acceptedDuration);
	}
	
	context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}

void PaxosLeaseAcceptor::OnProposeRequest(const PaxosLeaseMessage& imsg)
{
	PaxosLeaseMessage omsg;

	Log_Trace();
	
	if (state.accepted && state.acceptedExpireTime < Now())
	{
		EventLoop::Remove(&leaseTimeout);
		OnLeaseTimeout();
	}
	
	if (imsg.proposalID < state.promisedProposalID)
		omsg.ProposeRejected(RMAN->GetNodeID(), imsg.proposalID);
	else
	{
		state.accepted = true;
		state.acceptedProposalID = imsg.proposalID;
		state.acceptedLeaseOwner = imsg.leaseOwner;
		state.acceptedDuration = imsg.duration;
		state.acceptedExpireTime = Now() + state.acceptedDuration;
		
		leaseTimeout.Set(state.acceptedExpireTime);
		EventLoop::Reset(&leaseTimeout);
		
		omsg.ProposeAccepted(RMAN->GetNodeID(), imsg.proposalID);
	}
	
	context->GetTransport()->SendMessage(imsg.nodeID, omsg);
}

void PaxosLeaseAcceptor::OnLeaseTimeout()
{
	Log_Trace();
	
	state.OnLeaseTimeout();
}
