#ifndef PAXOSLEASE_H
#define PAXOSLEASE_H

#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
#include "PaxosLeaseMessage.h"
#include "PaxosLeaseProposer.h"
#include "PaxosLeaseAcceptor.h"
#include "PaxosLeaseLearner.h"

#define ACQUIRELEASE_TIMEOUT	2000	// msec
#define MAX_LEASE_TIME			7000	// msec

/*
===============================================================================

 PaxosLease

===============================================================================
*/

class PaxosLease
{
public:
	void					Init(QuorumContext* context);
	
	void					OnMessage(const PaxosLeaseMessage& msg);
	
	void					AcquireLease();
	bool					IsLeaseOwner();
	bool					IsLeaseKnown();
	unsigned				GetLeaseOwner();
	uint64_t				GetLeaseEpoch();
	
private:
	void					OnStartupTimeout();
	void					OnLearnLease();
	void					OnLeaseTimeout();

	QuorumContext*			context;
 	PaxosLeaseProposer		proposer;
	PaxosLeaseAcceptor		acceptor;
	PaxosLeaseLearner		learner;
	Countdown				startupTimeout;
	bool					acquireLease;
};

#endif
