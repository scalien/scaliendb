#include "HttpServer.h"
#include "System/IO/IOProcessor.h"

#define CONN_BACKLOG	10

void HttpServer::Init(int port)
{
	if (!TCPServer<HttpServer, HttpConn>::Init(port, CONN_BACKLOG))
		STOP_FAIL("Cannot initialize HttpServer", 1);
	handlers = NULL;
}

void HttpServer::Shutdown()
{
	Close();
}

void HttpServer::InitConn(HttpConn* conn)
{
	conn->Init(this);
}

void HttpServer::RegisterHandler(HttpHandler* handler)
{
	handler->nextHttpHandler = handlers;
	handlers = handler;
}

bool HttpServer::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	HttpHandler*	handler;
	bool			ret;
	
	// call each handler until one handles the request
	ret = false;
	for (handler = handlers; handler && !ret; handler = handler->nextHttpHandler)
		ret = handler->HandleRequest(conn, request);
	
	return ret;
}

