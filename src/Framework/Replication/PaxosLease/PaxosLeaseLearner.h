#ifndef PAXOSLEASELEARNER_H
#define PAXOSLEASELEARNER_H

#include "System/Common.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/ReplicationContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
#include "PaxosLeaseMessage.h"
#include "PaxosLeaseLearnerState.h"

/*
===============================================================================

 PaxosLeaseLearner

===============================================================================
*/

class PaxosLeaseLearner
{
public:
	void					Init(ReplicationContext* context);

	void					OnMessage(PaxosLeaseMessage* msg);
	void					OnLeaseTimeout();
	bool					IsLeaseOwner();
	bool					IsLeaseKnown();
	int						GetLeaseOwner();
	uint64_t				GetLeaseEpoch();
	void					SetOnLearnLease(const Callable& onLearnLeaseCallback);
	void					SetOnLeaseTimeout(const Callable& onLeaseTimeoutCallback);

private:
	void					OnLearnChosen(PaxosLeaseMessage* msg);
	void					CheckLease();

	Timer					leaseTimeout;
	Callable				onLearnLeaseCallback;
	Callable				onLeaseTimeoutCallback;
	PaxosLeaseLearnerState	state;	
	ReplicationContext*		context;
};

#endif
