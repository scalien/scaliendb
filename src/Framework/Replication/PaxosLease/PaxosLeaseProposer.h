#ifndef PAXOSLEASEPROPOSER_H
#define PAXOSLEASEPROPOSER_H

#include "System/Common.h"
#include "System/Events/CdownTimer.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/ReplicationContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
#include "PaxosLeaseMessage.h"
#include "States/PaxosLeaseProposerState.h"

/*
===============================================================================

 PaxosLeaseProposer

===============================================================================
*/

class PaxosLeaseProposer
{
public:
	void						Init(ReplicationContext* context);
	
	void						OnMessage(const PaxosLeaseMessage& msg);
	void						OnNewPaxosRound();
	void						OnAcquireLeaseTimeout();
	void						OnExtendLeaseTimeout();
	void						StartAcquireLease();
	void						StopAcquireLease();
	uint64_t					HighestProposalID();
	void						SetHighestProposalID(uint64_t highestProposalID);

private:
	void						BroadcastMessage(const PaxosLeaseMessage& msg);
	void						OnPrepareResponse(const PaxosLeaseMessage& msg);
	void						OnProposeResponse(const PaxosLeaseMessage& msg);
	void						StartPreparing();
	void						StartProposing();

	CdownTimer					acquireLeaseTimeout;
	Timer						extendLeaseTimeout;
	uint64_t					highestProposalID;	
	PaxosLeaseProposerState		state;
	ReplicationContext*			context;
};

#endif
