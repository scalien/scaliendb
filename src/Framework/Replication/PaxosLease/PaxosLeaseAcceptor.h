#ifndef PAXOSLEASEACCEPTOR_H
#define PAXOSLEASEACCEPTOR_H

#include "System/Common.h"
#include "System/Events/Timer.h"
#include "Framework/Transport/TCPWriter.h"
#include "Framework/Replication/ReplicationContext.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"
#include "States/PaxosLeaseAcceptorState.h"
#include "PaxosLeaseMessage.h"

/*
===============================================================================

 PaxosLeaseAcceptor

===============================================================================
*/

class PaxosLeaseAcceptor
{
public:
	void						Init(ReplicationContext* context);
	void						OnMessage(PaxosLeaseMessage* msg);
	void						OnLeaseTimeout();

private:
	void						OnPrepareRequest(PaxosLeaseMessage* msg);
	void						OnProposeRequest(PaxosLeaseMessage* msg);

	Timer						leaseTimeout;
	PaxosLeaseAcceptorState		state;
	ReplicationContext*			context;
};

#endif
