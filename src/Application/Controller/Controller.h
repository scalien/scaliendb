#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ControlConfigContext.h"
#include "Application/HTTP/HttpServer.h"

class Controller : public HttpHandler
{
public:

	// HttpHandler interface
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request);
	
	void			SetContext(ControlConfigContext* context);
	
private:
	ControlConfigContext*	context;
};

//ControlConfigContext => MASTER?
//ControlConfigContext => CLUSTER_HELLO
//(3)
//CONFIG_CREATE_CHUNK => ControlConfigContext => OK => MessageWriter
//CLUSTER_INFO => DataNodes
//
//no PRIMARY

#endif
