#ifndef PAXOSLEASE_H
#define PAXOSLEASE_H

#include "System/Events/Timer.h"
#include "Framework/Replication/ReplicationContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
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
	PaxosLease();

	void					Init(ReplicationContext* context);
	void					Shutdown();
	void					OnMessage();
	void					AcquireLease();
	bool					IsLeaseOwner();
	bool					IsLeaseKnown();
	unsigned				GetLeaseOwner();
	uint64_t				GetLeaseEpoch();
	void					SetOnLearnLease(const Callable& onLearnLeaseCallback);
	void					SetOnLeaseTimeout(const Callable& onLeaseTimeoutCallback);
	void					Stop();
	void					Continue();
	bool					IsActive();
	void					OnNewPaxosRound();
	void					OnLearnLease();
	void					OnLeaseTimeout();
	void					OnStartupTimeout();
	
private:
	bool					acquireLease;
	CdownTimer				startupTimeout;
	Callable				onLearnLeaseCallback;
	Callable				onLeaseTimeoutCallback;
 	PaxosLeaseProposer		proposer;
	PaxosLeaseAcceptor		acceptor;
	PaxosLeaseLearner		learner;
	ReplicationContext*		context;
};

#endif
