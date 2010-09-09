#include "SDBPServer.h"

#define CONN_BACKLOG	10

void SDBPServer::Init(int port)
{
	if (!TCPServer<SDBPServer, SDBPConnection>::Init(port, CONN_BACKLOG))
		STOP_FAIL(1, "Cannot initialize SDBPServer");
}

void SDBPServer::Shutdown()
{
	Close();
}

void SDBPServer::InitConn(SDBPConnection* conn)
{
	conn->Init(this);
}
