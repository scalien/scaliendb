#ifndef QUORUMTRANSPORT_H
#define QUORUMTRANSPORT_H

#include "Quorum.h"
#include "Framework/Replication/ReplicationTransport.h"
#include "Framework/Messaging/Message.h"

/*
===============================================================================

 QuorumTransport

===============================================================================
*/

class QuorumTransport
{
public:
	QuorumTransport();
	
	void					SetPriority(bool priority);
	void					SetReplicationTransport(ReplicationTransport* transport);
	void					SetQuorum(Quorum* quorum);
	
	Message*				GetMessage() const;

	void					SendMessage(uint64_t nodeID, const Message& msg);
	void					BroadcastMessage(const Message& msg);

private:
	bool					priority;
	Quorum*					quorum;
	ReplicationTransport*	transport;
};

#endif
