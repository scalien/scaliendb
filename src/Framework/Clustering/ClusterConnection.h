#ifndef CLUSTERCONNECTION_H
#define CLUSTERCONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Message.h"

#define MESSAGING_YIELD_TIME			10 // msec
#define MESSAGING_CONNECT_TIMEOUT		2000

class ClusterTransport;		// forward

/*
===============================================================================

 ClusterConnection

===============================================================================
*/

class ClusterConnection : public MessageConnection
{
public:
	enum Progress
	{
		INCOMING,		// connection established, nodeID not received, endpoint == unknown
		OUTGOING,		// connecting in progress, nodeID not sent
		READY			// connection established, other side's nodeID known
	};

	void				InitConnected(bool startRead = true);
	void				SetTransport(ClusterTransport* transport);

	void				SetNodeID(uint64_t nodeID);
	void				SetEndpoint(Endpoint& endpoint);

	uint64_t			GetNodeID();
	Progress			GetProgress();

	void				Connect();
	void				OnConnect();
	void				OnClose();

	/* MessageConnection interface									*/
	/* Returns whether the connection was closed and deleted		*/
	virtual bool		OnMessage(ReadBuffer& msg);

private:
	Progress			progress;
	uint64_t			nodeID;
	ClusterTransport*	transport;
};

#endif
