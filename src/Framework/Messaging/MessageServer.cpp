#include "MessageServer.h"
#include "MessageTransport.h"

bool MessageServer::Init(int port)
{
	return TCPServer<MessageServer, MessageConnection>::Init(port);
}

void MessageServer::InitConn(MessageConnection* conn)
{
	conn->SetTransport(transport);
	conn->InitConnected();
}

void MessageServer::SetTransport(MessageTransport* transport_)
{
	transport = transport_;
}

bool MessageServer::IsManaged()
{
	return false;
}
