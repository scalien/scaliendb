#include "HTTPServer.h"
#include "System/IO/IOProcessor.h"

#define CONN_BACKLOG	10

void HTTPServer::Init(int port)
{
	if (!TCPServer<HTTPServer, HTTPConnection>::Init(port, CONN_BACKLOG))
		STOP_FAIL("Cannot initialize HTTPServer", 1);
	handlers = NULL;
}

void HTTPServer::Shutdown()
{
	Close();
}

void HTTPServer::InitConn(HTTPConnection* conn)
{
	conn->Init(this);
}

void HTTPServer::RegisterHandler(HTTPHandler* handler)
{
	handler->nextHTTPHandler = handlers;
	handlers = handler;
}

bool HTTPServer::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
	HTTPHandler*	handler;
	bool			ret;
	
	// call each handler until one handles the request
	ret = false;
	for (handler = handlers; handler && !ret; handler = handler->nextHTTPHandler)
		ret = handler->HandleRequest(conn, request);
	
	return ret;
}

