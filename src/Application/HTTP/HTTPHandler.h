#ifndef HTTPHANDLER_H
#define HTTPHANDLER_H

class HTTPConnection;	// forward
class HTTPRequest;		// forward

/*
===============================================================================================

 HTTPHandler is the generic handler interface to HTTPServer

===============================================================================================
*/

class HTTPHandler
{
public:
	virtual ~HTTPHandler() {}
	
	virtual bool	HandleRequest(HTTPConnection* conn, HTTPRequest& request) = 0;
	
	HTTPHandler*	nextHTTPHandler;
};

#endif
