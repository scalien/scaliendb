#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ControllerConfigContext.h"
#include "Application/HTTP/HttpServer.h"

class Controller : public HttpHandler
{
public:

	// HttpHandler interface
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request);
	
	void			SetContext(ControllerConfigContext* context);
	
private:
	ControllerConfigContext*	context;
};

#endif
