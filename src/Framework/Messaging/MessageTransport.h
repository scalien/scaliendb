#ifndef MESSAGETRANSPORT_H
#define MESSAGETRANSPORT_H

#include "MessageServer.h"
#include "Message.h"

class MessageTransport
{
public:
	virtual ~MessageTransport() {}

	void				Init(uint64_t nodeID, Endpoint& endpoint);
		
	void				OnIncomingConnection(MessageConnection* conn);
	void				OnOutgoingConnection(MessageConnection* conn);
	void				OnClose(MessageConnection* conn);
	void				OnRead(MessageConnection* conn, ReadBuffer msg);

	void				AddEndpoint(uint64_t nodeID, Endpoint endpoint);
	unsigned			GetNumEndpoints();

	void				SendMessage(uint64_t nodeID, const Buffer& prefix, Message& msg);
	void				SendPriorityMessage(uint64_t nodeID, const Buffer& prefix, Message& msg);

	virtual void		OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)	= 0;
	virtual void		OnMessage(ReadBuffer msg)										= 0;

private:
	Node*				GetNode(uint64_t nodeID);
	void				DeleteNode(Node* node);
	
	InList<Node>		nodes;
	MessageServer		server;
	uint64_t			selfNodeID;
	Endpoint			selfEndpoint;
};

#endif
