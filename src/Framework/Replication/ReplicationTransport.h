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
	uint64_t		nodeID;
	Endpoint		endpoint;
	MessageWriter	writer;
	
	Node*			next;
	Node*			prev;
};

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport
{
public:
	void			Start();
	void			Shutdown();

	void			AddEndpoint(uint64_t nodeID, Endpoint endpoint);
	unsigned		GetNumEndpoints();
	Endpoint		GetEndpoint(uint64_t nodeID);

	void			SendMessage(uint64_t nodeID, const Buffer& prefix, const Message& msg);
	void			SendPriorityMessage(uint64_t nodeID, const Buffer& prefix, const Message& msg);

	ReadBuffer		GetMessage();

private:
	void			OnRead();

	MessageReader	reader;
	InList<Node>	nodes;
	ReadBuffer		readBuffer;
};

#endif
