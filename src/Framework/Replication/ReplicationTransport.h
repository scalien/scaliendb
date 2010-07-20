#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

#include "System/Containers/InList.h"
#include "Framework/Messaging/MessageReader.h"
#include "Framework/Messaging/MessageWriter.h"

class Message; // forward

/*
===============================================================================

 Node

===============================================================================
*/

struct Node
{
	uint64_t			nodeID;
	Endpoint			endpoint;
	MessageWriter		writer;
	
	Node*				next;
	Node*				prev;
};

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport
{
public:

	void					Init();
	void					Shutdown();

	void					AddNode(uint64_t nodeID, const Endpoint& endpoint);

	void					SendMessage(uint64_t nodeID, const Message& msg);
	void					SendPriorityMessage(uint64_t nodeID, const Message& msg);

//	void					BroadcastMessage(const Message& msg);
//	void					BroadcastPriorityMessage(const Message& msg);

private:
	void					OnRead();

	MessageReader			reader;
	InList<Node>			nodes;
};

#endif
