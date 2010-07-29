#ifndef MESSAGESERVER_H
#define MESSAGESERVER_H

#include "Framework/TCP/TCPServer.h"
#include "MessageConnection.h"

/*
===============================================================================

 MessageServer

===============================================================================
*/

class MessageServer : public TCPServer<MessageServer, MessageConnection>
{
public:
	bool				Init(int port);
	void				InitConn(MessageConnection* conn);
	
	void				SetTransport(MessageTransport* transport);
	bool				IsManaged();

private:	
	MessageTransport*	transport;
};

#endif
