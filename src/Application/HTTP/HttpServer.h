#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

#include "Framework/TCP/TCPServer.h"
#include "HttpConn.h"

class KeyspaceDB;
class HttpRequest;
class HttpServer;

class HttpHandler
{
public:
	virtual ~HttpHandler() {}
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request) = 0;
	
	HttpHandler*	nextHttpHandler;
};

class HttpServer : public TCPServer<HttpServer, HttpConn>
{
public:
	void			Init(int port);
	void			Shutdown();
	
	void			InitConn(HttpConn* conn);

	void			RegisterHandler(HttpHandler* handler);
	bool			HandleRequest(HttpConn* conn, const HttpRequest& request);
	
private:
	HttpHandler*	handlers;
};

#endif
