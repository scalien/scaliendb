#include <stdio.h>

#include "Application/HTTP/HttpServer.h"
#include "Application/HTTP/HttpConsts.h"
#include "System/Events/EventLoop.h"

class SimpleHandler : public HttpHandler
{
public:
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request);
};

bool SimpleHandler::HandleRequest(HttpConn* conn, const HttpRequest& /*request*/)
{
	conn->Response(HTTP_STATUS_CODE_OK, "hello", 5);
	return true;
}

int main(void)
{
//	configFile.Init("");
	
	HttpServer		httpServer;
	SimpleHandler	handler;
		
	Log_SetTarget(LOG_TARGET_STDOUT);
	Log_SetTrace(true);
	Log_SetTimestamping(true);
	
	IOProcessor::Init(1024);
	
	httpServer.Init(8080);
	httpServer.RegisterHandler(&handler);

	EventLoop::Init();
	EventLoop::Run();
	EventLoop::Shutdown();
}
