#ifndef PAXOSACCEPTOR_H
#define PAXOSACCEPTOR_H

#include "System/Common.h"
#include "PaxosMessage.h"
#include "Framework/Replication/ReplicationContext.h"
#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "States/PaxosAcceptorState.h"

/*
===============================================================================

 PaxosAcceptor

===============================================================================
*/

class PaxosAcceptor
{
public:
	void						Init(ReplicationContext* context);
	void						OnMessage(const PaxosMessage& msg);

private:
	void						OnPrepareRequest(const PaxosMessage& msg);
	void						OnProposeRequest(const PaxosMessage& msg);
	void						OnStateWritten();

	void						ReadState();
	void						WriteState();

	ReplicationContext*			context;
	PaxosAcceptorState			state;
	uint64_t					paxosID;
	uint64_t					senderID;
	uint64_t					writtenPaxosID;
};

#endif
