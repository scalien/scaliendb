#ifndef REPLICATEDLOG_H
#define REPLICATEDLOG_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Paxos/PaxosProposer.h"
#include "Framework/Replication/Paxos/PaxosAcceptor.h"
#include "Framework/Replication/Paxos/PaxosLearner.h"
#include "LogCache.h"

#define CATCHUP_TIMEOUT	5000

/*
===============================================================================

 ReplicatedLog

===============================================================================
*/

class ReplicatedLog
{
public:
	void					Init(QuorumContext* context);
	
	void					OnMessage(const PaxosMessage& msg);
	uint64_t				GetPaxosID() const;
	
	void					RegisterPaxosID(uint64_t paxosID, unsigned nodeID);
	void					OnMessage();
	void					Append(const Buffer& value);
	bool					IsAppending();
//	bool					IsMultiRound();
//	uint64_t				GetLastRound_Length();
//	uint64_t				GetLastRound_Time();
//	uint64_t				GetLastRound_Thruput();

private:
	void					OnPrepareRequest(const PaxosMessage& msg);
	void					OnPrepareResponse(const PaxosMessage& msg);
	void					OnProposeRequest(const PaxosMessage& msg);
	void					OnProposeResponse(const PaxosMessage& msg);
	void					OnLearnChosen(const PaxosMessage& msg);
	
	void					OnRequestChosen(const PaxosMessage& msg);
//	void					OnStartCatchup();

	void					OnRequest(const PaxosMessage& msg);
	void					NewPaxosRound();
	void					RequestChosen();

	QuorumContext*			context;

	PaxosProposer			proposer;
	PaxosAcceptor			acceptor;
	PaxosLearner			learner;
	uint64_t				lastRequestChosenTime;
	uint64_t				lastRequestChosenPaxosID;

	LogCache				logCache;
//	ReplicatedDB*			replicatedDB;
	
//	uint64_t				lastStarted;
//	uint64_t				lastLength;
//	uint64_t				lastTook;
//	uint64_t				thruput;
};
#endif
