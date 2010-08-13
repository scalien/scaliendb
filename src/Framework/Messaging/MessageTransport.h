#ifndef MESSAGETRANSPORT_H
#define MESSAGETRANSPORT_H

#include "MessageServer.h"
#include "Message.h"

class MessageTransport
{
public:
	virtual ~MessageTransport() {}

	void				Init(uint64_t nodeID, Endpoint& endpoint);
		
	void				AddEndpoint(uint64_t nodeID, Endpoint endpoint);

	void				SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg);
	void				SendPriorityMessage(uint64_t nodeID, Buffer& prefix, Message& msg);

	virtual void		OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)	= 0;
	virtual void		OnMessage(ReadBuffer msg)										= 0;

	uint64_t			GetNodeID();
	Endpoint&			GetEndpoint();

private:
	void				AddConnection(MessageConnection* conn);
	MessageConnection*	GetConnection(uint64_t nodeID);
	void				DeleteConnection(MessageConnection* conn);

	InList<MessageConnection>	conns;
	MessageServer				server;
	uint64_t					nodeID;
	Endpoint					endpoint;

	friend class MessageConnection;
};

#endif
