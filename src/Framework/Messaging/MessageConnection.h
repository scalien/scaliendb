#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

#define MESSAGING_YIELD_TIME			10 // msec
#define MESSAGING_CONNECT_TIMEOUT		2000

class MessageTransport;		// forward

/*
===============================================================================

MessageConnection

===============================================================================
*/

class MessageConnection : public TCPConnection
{
public:
	MessageConnection();

	enum Progress
	{
		INCOMING,			// incoming connection established, nodeID not sent, node->endpoint == unknown
		OUTGOING,			// connecting in progress
		READY				// connection established, other side's nodeID known
	};

	void				InitConnected(bool startRead = true);
	void				SetTransport(MessageTransport* transport);

	void				SetNodeID(uint64_t nodeID);
	void				SetEndpoint(Endpoint& endpoint);

	uint64_t			GetNodeID();
	Endpoint			GetEndpoint();
	Progress			GetProgress();

	void				Write(Buffer& buffer);
	void				WritePriority(Buffer& buffer);
	void				Write(Buffer& prefix, Message& msg);
	void				WritePriority(Buffer& prefix, Message& msg);

	// read:
	virtual void		OnClose();
	virtual void		OnRead();
	void				OnMessage(ReadBuffer& msg);
	void				OnResumeRead();

	// write:
	void				Connect();
	void				OnConnect();
	void				OnConnectTimeout();

	virtual void		Close();
	
protected:
	Progress			progress;
	uint64_t			nodeID;
	Endpoint			endpoint;

	MessageTransport*	transport;
	Countdown			resumeRead;
};

#endif
