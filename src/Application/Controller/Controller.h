#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ControlConfigContext.h"
#include "Application/HTTP/HttpServer.h"
#include "ClusterMessage.h"
#include "ConfigMessage.h"

class Controller : public HttpHandler
{
public:

	// HttpHandler interface
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request);
	
	void			SetContext(ControlConfigContext* context);

	//void			OnClusterMessage(const ClusterMessage& msg);
	void			OnConfigMessage(const ConfigMessage& msg);
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
