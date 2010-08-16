#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

#include "Framework/TCP/TCPServer.h"
#include "HTTPConnection.h"
#include "HTTPConsts.h"

class KeyspaceDB;	// forward
class HTTPRequest;	// forward
class HTTPServer;	// forward

/*
===============================================================================

 HTTPHandler is the generic handler interface to HTTPServer

===============================================================================
*/

class HTTPHandler
{
public:
	virtual ~HTTPHandler() {}
	
	virtual bool	HandleRequest(HTTPConnection* conn, HTTPRequest& request) = 0;
	
	HTTPHandler*	nextHTTPHandler;
};

/*
===============================================================================

 HTTPServer

===============================================================================
*/

class HTTPServer : public TCPServer<HTTPServer, HTTPConnection>
{
public:
	void			Init(int port);
	void			Shutdown();
	
	void			InitConn(HTTPConnection* conn);

	void			RegisterHandler(HTTPHandler* handler);
	bool			HandleRequest(HTTPConnection* conn, HTTPRequest& request);
	
private:
	HTTPHandler*	handlers;
};

#endif
