#ifndef PAXOSLEASELEARNER_H
#define PAXOSLEASELEARNER_H

#include "System/Common.h"
#include "System/Events/Timer.h"
#include "Framework/Replication/Quorums/QuorumContext.h"
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
	void						Init(QuorumContext* context);

	bool						IsLeaseOwner();
	bool						IsLeaseKnown();
	uint64_t					GetLeaseOwner();
	void						SetOnLearnLease(const Callable& onLearnLeaseCallback);
	void						SetOnLeaseTimeout(const Callable& onLeaseTimeoutCallback);

	void						OnMessage(const PaxosLeaseMessage& msg);

private:
	void						OnLeaseTimeout();
	void						OnLearnChosen(const PaxosLeaseMessage& msg);
	void						CheckLease();

	QuorumContext*				context;
	PaxosLeaseLearnerState		state;	
	Timer						leaseTimeout;
	Callable					onLearnLeaseCallback;
	Callable					onLeaseTimeoutCallback;
};

#endif
