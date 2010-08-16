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
		INCOMING,			// connection established, nodeID not received, endpoint == unknown
		OUTGOING,			// connecting in progress, nodeID not sent
		READY				// connection established, other side's nodeID known
	};

	void				InitConnected(bool startRead = true);
	void				SetTransport(MessageTransport* transport);

	void				SetNodeID(uint64_t nodeID);
	void				SetEndpoint(Endpoint& endpoint);

	uint64_t			GetNodeID();
	Progress			GetProgress();

	void				Write(Buffer& msg);
	void				WritePriority(Buffer& msg);
	void				Write(Buffer& prefix, Buffer& msg);
	void				WritePriority(Buffer& prefix, Buffer& msg);

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
