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
	void					SetPrefix(const Buffer& prefix);
	void					SetQuorum(Quorum* quorum);
	
	ReadBuffer				GetMessage() const;

	void					SendMessage(uint64_t nodeID, const Message& msg);
	void					BroadcastMessage(const Message& msg);

private:
	bool					priority;
	Buffer					prefix;
	Quorum*					quorum;
};

#endif
