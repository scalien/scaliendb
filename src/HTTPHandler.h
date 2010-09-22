#ifndef HTTPHANDLER_H
#define HTTPHANDLER_H

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
