#ifndef PAXOSLEASEPROPOSER_H
#define PAXOSLEASEPROPOSER_H

#include "System/Common.h"
#include "System/Events/CdownTimer.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/ReplicationContext.h"
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

	void						StartAcquireLease();
	void						StopAcquireLease();
	uint64_t					GetHighestProposalID();
	void						SetHighestProposalID(uint64_t highestProposalID);

private:
	void						OnAcquireLeaseTimeout();
	void						OnExtendLeaseTimeout();
	void						OnPrepareResponse(const PaxosLeaseMessage& msg);
	void						OnProposeResponse(const PaxosLeaseMessage& msg);

	void						BroadcastMessage(const PaxosLeaseMessage& msg);
	void						StartPreparing();
	void						StartProposing();

	PaxosLeaseProposerState		state;
	ReplicationContext*			context;
	uint64_t					highestProposalID;	
	CdownTimer					acquireLeaseTimeout;
	Timer						extendLeaseTimeout;
};

#endif
