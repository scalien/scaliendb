#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

#define MESSAGING_YIELD_TIME			10 // msec
#define MESSAGING_CONNECT_TIMEOUT		2000

class MessageConnection;	// forward
class MessageTransport;		// forward

struct Node
{
	Node()	{ next = prev = this; }
	enum State
	{
		INCOMING,			// incoming connection established, nodeID not sent, node->endpoint == unknown
		CONNECTING,			// connecting in progress
		READY				// connection established, other side's nodeID known
	};
	
	State					state;
	uint64_t				nodeID;
	Endpoint				endpoint;
	MessageConnection*		conn;
	
	Node*					next;
	Node*					prev;
};

/*
===============================================================================

MessageConnection

===============================================================================
*/

class MessageConnection : public TCPConnection
{
public:
	MessageConnection();
	
	void				SetTransport(MessageTransport* transport);

	void				Write(const Buffer& buffer);
	void				Write(const Buffer& prefix, const Message& msg);
	void				WritePriority(const Buffer& prefix, const Message& msg);

	// read:
	virtual void		OnClose();
	virtual void		OnRead();
	void				OnResumeRead();

	// write:
	void				Connect(const Endpoint& endpoint);
	void				OnConnect();
	void				OnConnectTimeout();

	virtual void		Close();
	
	Node*				GetNode();
	void				SetNode(Node* node);

protected:
	MessageTransport*	transport;
	Countdown			resumeRead;
	Endpoint			endpoint;
	Node*				node;
};

#endif
