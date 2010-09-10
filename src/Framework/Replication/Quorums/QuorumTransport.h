#ifndef QUORUMTRANSPORT_H
#define QUORUMTRANSPORT_H

#include "Quorum.h"
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
	void					SetQuorum(Quorum* quorum);
	void					SetContextID(uint64_t contextID);
	
	void					SendMessage(uint64_t nodeID, Message& msg);
	void					BroadcastMessage(Message& msg);

private:
	bool					priority;
	uint64_t				contextID;
	Buffer					prefix;
	Quorum*					quorum;
};

#endif
