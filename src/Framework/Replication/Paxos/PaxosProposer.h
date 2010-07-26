#ifndef PAXOSPROPOSER_H
#define PAXOSPROPOSER_H

#include "System/Common.h"
#include "System/Events/Countdown.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosMessage.h"
#include "States/PaxosProposerState.h"

class ReplicatedLog; // forward

#define PAXOS_TIMEOUT	3000

/*
===============================================================================

 PaxosProposer

===============================================================================
*/

class PaxosProposer
{
public:
	void						Init(QuorumContext* context);
	
	void						OnMessage(const PaxosMessage& msg);
	void						OnPrepareTimeout();
	void						OnProposeTimeout();
	
	void						Propose(const Buffer& value);
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

	QuorumContext*				context;
	QuorumVote*					vote;
	PaxosProposerState			state;
	Countdown					prepareTimeout;
	Countdown					proposeTimeout;
	
	friend class ReplicatedLog;
};

#endif
