#ifndef HTTPCONTROLLERCONTEXT_H
#define HTTPCONTROLLERCONTEXT_H

#include "Application/HTTP/HTTPServer.h"

class Controller;	// forward

/*
===============================================================================

 HTTPControllerContext

===============================================================================
*/

class HTTPControllerContext : public HTTPHandler
{
public:
	void			SetController(Controller* controller);
	
	// HTTPHandler interface
	virtual bool	HandleRequest(HTTPConnection* conn, HTTPRequest& request);

private:
	Controller*		controller;
};

#endif
