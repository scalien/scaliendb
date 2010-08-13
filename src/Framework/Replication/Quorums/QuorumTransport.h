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
	void					SetPrefix(Buffer& prefix);
	void					SetQuorum(Quorum* quorum);
	
	ReadBuffer				GetMessage();

	void					SendMessage(uint64_t nodeID,Message& msg);
	void					BroadcastMessage(Message& msg);

private:
	bool					priority;
	Buffer					prefix;
	Quorum*					quorum;
};

#endif
