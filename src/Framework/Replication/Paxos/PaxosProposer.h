#ifndef PAXOSPROPOSER_H
#define PAXOSPROPOSER_H

#include "System/Common.h"
#include "System/Events/CdownTimer.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/ReplicationContext.h"
#include "PaxosMessage.h"
#include "States/PaxosProposerState.h"

#define PAXOS_TIMEOUT	3000

/*
===============================================================================

 PaxosProposer

===============================================================================
*/

class PaxosProposer
{
public:
	void						Init(ReplicationContext* context);
	
	void						OnMessage(const PaxosMessage& msg);
	void						OnPrepareTimeout();
	void						OnProposeTimeout();
	
	bool						Propose(const Buffer& value);
	void						Stop();
	bool						IsActive();	

private:
	void						OnPrepareResponse(const PaxosMessage& msg);
	void						OnProposeResponse(const PaxosMessage& msg);

	void						BroadcastMessage(const PaxosMessage& msg);
	void						StopPreparing();
	void						StopProposing();
	void						StartPreparing();
	void						StartProposing();

	PaxosProposerState			state;
	ReplicationContext*			context;
	uint64_t					paxosID;
	CdownTimer					prepareTimeout;
	CdownTimer					proposeTimeout;
};

#endif
